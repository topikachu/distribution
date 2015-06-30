package ossapi

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type OssApi struct {
	region, accessKeyId, accessKeySecret, bucket string
	secure                                       bool
}

type part struct {
	PartNumber int
	ETag       string
}
type partSlice []part
type UploadContext struct {
	Key, UploadId string
	Parts         partSlice
}

func (s partSlice) Len() int           { return len(s) }
func (s partSlice) Less(i, j int) bool { return s[i].PartNumber < s[j].PartNumber }
func (s partSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (context *UploadContext) addPart(partNumber int, etag string) {
	newpart := part{partNumber, etag}
	for i, part := range context.Parts {
		if partNumber == part.PartNumber {
			context.Parts[i] = newpart
			return
		}
	}
	context.Parts = append(context.Parts, newpart)
}

type request struct {
	method  string
	object  string
	params  url.Values
	headers http.Header
	baseurl string
	payload []byte
}

func New(region, accessKeyId, accessKeySecret, bucket string, secure bool) *OssApi {
	return &OssApi{region, accessKeyId, accessKeySecret, bucket, secure}
}

func (api *OssApi) ListFiles(object, delimiter, marker string, max int) ([]string, []string, string, error) {
	object = noramilizeObject(object)
	params := make(map[string][]string)
	if object != "" {
		params["prefix"] = []string{object}
	}
	if delimiter != "" {
		params["delimiter"] = []string{delimiter}
	}
	if marker != "" {
		params["marker"] = []string{marker}
	}

	if max > 0 && max < 1000 {
		params["max-keys"] = []string{strconv.Itoa(max)}
	}
	req := &request{
		method: "GET",
		params: params,
	}

	var resp struct {
		Nextmarker  string
		IsTruncated bool
		Contents    []struct {
			Key string
		}
		CommonPrefixes struct {
			Prefix []string
		}
	}

	err := api.query(req, &resp)
	if err != nil {
		return nil, nil, "", err
	}

	var contents []string

	for _, content := range resp.Contents {
		contents = append(contents, content.Key)
	}
	nextMarker := ""

	if resp.IsTruncated {
		nextMarker = resp.Nextmarker
	}

	return contents, resp.CommonPrefixes.Prefix, nextMarker, nil
}

func (api *OssApi) PutObject(object string, contents []byte, contentType string) error {
	object = noramilizeObject(object)
	req := &request{
		method: "PUT",
		object: object,
		headers: map[string][]string{
			"Content-Type": {contentType},
		},
		payload: contents,
	}
	return api.query(req, nil)
	return nil
}

type Header struct {
	http.Header
}

func (header *Header) GetContentLength() (int64, error) {
	return strconv.ParseInt(header.Get("Content-Length"), 10, 64)

}

func (header *Header) GetDate() (time.Time, error) {
	return http.ParseTime(header.Get("Date"))

}

func (header *Header) GetLastModified() (time.Time, error) {
	return http.ParseTime(header.Get("Last-Modified"))
}

func (api *OssApi) GetObjectMetadata(object string) (*Header, error) {
	object = noramilizeObject(object)
	req := &request{
		method: "HEAD",
		object: object,
	}
	hresp, err := api.rawQuery(req)
	if err != nil {
		return nil, err
	}
	defer hresp.Body.Close()
	return &Header{hresp.Header}, nil
}

type ReaderWithBytes struct {
	io.ReadCloser
	bytes []byte
}

func (r *ReaderWithBytes) Bytes() []byte {
	if r.bytes != nil {
		return r.bytes
	}
	var buffer bytes.Buffer
	buffer.ReadFrom(r)
	r.bytes = buffer.Bytes()
	return r.bytes
}

func (api *OssApi) GetObjectRange(object string, start, end int64) (*ReaderWithBytes, int, error) {
	object = noramilizeObject(object)
	var headers = make(map[string][]string)
	if start >= 0 || end >= 0 {
		if start < 0 {
			start = 0
		}
		if end >= 0 {
			headers["Range"] = []string{fmt.Sprintf("bytes=%d-%d", start, end)}
		} else {
			headers["Range"] = []string{fmt.Sprintf("bytes=%d-", start)}
		}
	}
	req := &request{
		method:  "GET",
		object:  object,
		headers: headers,
	}
	hresp, err := api.rawQuery(req)
	if err != nil {
		return nil, -1, err
	}

	return &ReaderWithBytes{hresp.Body, nil}, hresp.StatusCode, nil
}

func (api *OssApi) GetObject(object string) ([]byte, error) {
	object = noramilizeObject(object)
	r, _, err := api.GetObjectRange(object, -1, -1)
	if err != nil {
		return nil, err
	}
	return r.Bytes(), nil
}

func (api *OssApi) InitMultipartUpload(object, contentType string) (*UploadContext, error) {
	object = noramilizeObject(object)
	req := &request{
		method: "POST",
		object: object,
		headers: map[string][]string{
			"Content-Type": {contentType},
		},
		params: map[string][]string{
			"uploads": {""},
		},
	}

	var resp struct {
		UploadId string `xml:"UploadId"`
	}
	err := api.query(req, &resp)
	if err != nil {
		return nil, err
	}
	return &UploadContext{
		Key:      object,
		UploadId: resp.UploadId,
	}, nil
}

type ListMultipartUploadsMarker struct {
	Key, UploadId string
}

func (api *OssApi) ListMultipartUploads(object string, marker *ListMultipartUploadsMarker, max int) ([]*UploadContext, *ListMultipartUploadsMarker, error) {
	object = noramilizeObject(object)
	if strings.HasPrefix(object, "/") {
		object = strings.TrimPrefix(object, "/")
	}
	params := map[string][]string{
		"uploads": {""},
	}
	if object != "" {
		params["prefix"] = []string{object}
	}
	if marker != nil {
		if marker.Key != "" {
			params["key-marker"] = []string{marker.Key}
		}
		if marker.UploadId != "" {
			params["upload-id-marker"] = []string{marker.UploadId}
		}
	}

	if max > 0 && max < 1000 {
		params["max-uploads"] = []string{strconv.Itoa(max)}
	}
	req := &request{
		method: "GET",
		params: params,
	}

	var resp struct {
		NextKeyMarker      string
		NextUploadIdMarker string
		IsTruncated        bool
		Upload             []struct {
			Key      string
			UploadId string
		}
	}

	err := api.query(req, &resp)
	if err != nil {
		return nil, nil, err
	}
	var contexts []*UploadContext
	for _, upload := range resp.Upload {
		contexts = append(contexts, &UploadContext{
			Key:      upload.Key,
			UploadId: upload.UploadId,
		})
	}

	var nextMarker *ListMultipartUploadsMarker
	if resp.IsTruncated {
		nextMarker = &ListMultipartUploadsMarker{resp.NextKeyMarker, resp.NextUploadIdMarker}
	} else {
		nextMarker = nil
	}

	return contexts, nextMarker, nil
}

func (api *OssApi) FetchMultipartUploadParts(context *UploadContext) error {
	req := &request{
		method: "GET",
		object: context.Key,
		params: map[string][]string{
			"uploadId": {context.UploadId},
		},
	}

	var resp struct {
		Part []struct {
			PartNumber int
			ETag       string
		}
	}

	err := api.query(req, &resp)
	if err != nil {
		return err
	}

	for _, part := range resp.Part {
		context.addPart(part.PartNumber, part.ETag)
	}
	return nil
}

func (api *OssApi) UploadMultipart(context *UploadContext, contents []byte, partNumber int) error {
	req := &request{
		method: "PUT",
		object: context.Key,

		params: map[string][]string{
			"partNumber": {strconv.Itoa(partNumber)},
			"uploadId":   {context.UploadId},
		},
		payload: contents,
	}
	hresp, err := api.rawQuery(req)
	if err != nil {
		return err
	}
	etag := hresp.Header.Get("ETag")
	context.addPart(partNumber, etag)
	return nil
}

func (api *OssApi) CompleteMultipart(context *UploadContext) error {

	var completeUpload struct {
		XMLName xml.Name  `xml:"CompleteMultipartUpload"`
		Parts   partSlice `xml:"Part"`
	}
	completeUpload.Parts = context.Parts
	sort.Sort(completeUpload.Parts)
	data, err := xml.Marshal(&completeUpload)
	if err != nil {
		return err
	}
	req := &request{
		method: "POST",
		object: context.Key,

		params: map[string][]string{
			"uploadId": {context.UploadId},
		},
		payload: data,
	}
	return api.query(req, nil)
}

func (api *OssApi) UploadCopyMultipart(context *UploadContext, sourceBucket, sourceObject string, start, end int64, partNumber int) (int64, error) {
	sourceObject = noramilizeObject(sourceObject)
	if sourceBucket == "" {
		sourceBucket = api.bucket
	}
	headers := map[string][]string{
		"x-oss-copy-source": {"/" + sourceBucket + "/" + sourceObject},
	}
	if start >= 0 || end >= 0 {
		if start < 0 {
			start = 0
		}
		if end >= 0 {
			headers["x-oss-copy-source-range"] = []string{fmt.Sprintf("bytes=%d-%d", start, end)}
		} else {
			headers["x-oss-copy-source-range"] = []string{fmt.Sprintf("bytes=%d-", start)}
		}
	}
	req := &request{
		method:  "PUT",
		object:  context.Key,
		headers: headers,
		params: map[string][]string{
			"partNumber": {strconv.Itoa(partNumber)},
			"uploadId":   {context.UploadId},
		},
	}

	hresp, err := api.rawQuery(req)
	if err != nil {
		return 0, err
	}
	var resp struct {
		ETag string
	}
	err = xml.NewDecoder(hresp.Body).Decode(&resp)
	hresp.Body.Close()
	if err != nil {
		return 0, err
	}
	contentRange := hresp.Header.Get("Content-Range")
	re := regexp.MustCompile("bytes (\\d+)-(\\d+)/(\\d+)")
	matchResult := re.FindAllStringSubmatch(contentRange, -1)
	remaining := int64(0)
	if len(matchResult) != 0 {
		total, _ := strconv.ParseInt(matchResult[0][3], 10, 64)
		read, _ := strconv.ParseInt(matchResult[0][2], 10, 64)
		remaining = total - read - 1
	}
	context.addPart(partNumber, resp.ETag)
	return remaining, nil

}

func (api *OssApi) Copy(sourceBucket, sourceObject, target, contentType string, chunkSize int64) error {
	sourceObject = noramilizeObject(sourceObject)
	target = noramilizeObject(target)
	if chunkSize < 100*1024 {
		chunkSize = 100 * 1024
	}

	context, err := api.InitMultipartUpload(target, contentType)
	start := int64(0)
	end := int64(start + chunkSize - 1)
	partNumber := 1
	for {
		remaining, err := api.UploadCopyMultipart(context, sourceBucket, sourceObject, start, end, partNumber)
		if err != nil {
			return err
			defer api.AbortMultipart(context)
		}
		if remaining == 0 {
			break
		}
		start += chunkSize
		if remaining > chunkSize {
			end += chunkSize
		} else {
			end = -1
		}
		partNumber++
	}
	err = api.CompleteMultipart(context)
	if err != nil {
		defer api.AbortMultipart(context)
		return err
	}
	return nil
}

func (api *OssApi) AbortMultipart(context *UploadContext) error {
	req := &request{
		method: "DELETE",
		object: context.Key,
		params: map[string][]string{
			"uploadId": {context.UploadId},
		},
	}
	return api.query(req, nil)
}

func (api *OssApi) Delete(objects ...string) error {
	type Object struct {
		Key string
	}
	var multipleDelete struct {
		XMLName xml.Name `xml:"Delete"`
		Quiet   bool
		Objects []Object `xml:"Object"`
	}
	multipleDelete.Quiet = true
	for _, object := range objects {
		object = noramilizeObject(object)
		multipleDelete.Objects = append(multipleDelete.Objects, Object{object})
	}
	data, _ := xml.Marshal(&multipleDelete)
	req := &request{
		method: "POST",
		params: map[string][]string{
			"delete": {""},
		},
		payload: data,
	}
	return api.query(req, nil)
}

func (api *OssApi) GeneratePresignedUrl(object string, expiration int64) string {
	object = noramilizeObject(object)
	req := &request{
		method: "GET",
		params: map[string][]string{
			"Expires": {strconv.FormatInt(time.Now().Unix()+expiration*int64(time.Second), 10)},
		},
		object: object,
	}

	signature := api.sign(req)
	req.params["OSSAccessKeyId"] = []string{api.accessKeyId}
	req.params["Signature"] = []string{signature}

	return api.baseUrl() + "/" + object + "?" + url.Values(req.params).Encode()
}

func noramilizeObject(object string) string {
	if strings.HasPrefix(object, "/") {
		object = strings.TrimLeft(object, "/")
	}
	return object
}
