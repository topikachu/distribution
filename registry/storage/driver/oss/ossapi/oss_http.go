package ossapi

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"time"
)

// prepare sets up req to be delivered to S3.
func (api *OssApi) prepare(req *request) error {
	if req.method == "" {
		req.method = "GET"
	}
	if req.headers == nil {
		req.headers = make(map[string][]string)
	}

	if len(req.payload) != 0 {
		digest := md5.New()
		io.Copy(digest, bytes.NewReader(req.payload))
		sum := digest.Sum(nil)
		md5b64 := base64.StdEncoding.EncodeToString(sum)
		req.headers["Content-MD5"] = []string{md5b64}
		req.headers["Content-Length"] = []string{strconv.FormatInt(int64(len(req.payload)), 10)}
	}

	req.baseurl = api.baseUrl()
	log.Debugf("baseurl is %s", req.baseurl)
	u, err := url.Parse(req.baseurl)
	if err != nil {
		return fmt.Errorf("bad oss endpoint URL %q: %v", req.baseurl, err)
	}
	req.headers["Host"] = []string{u.Host}
	req.headers["Date"] = []string{time.Now().In(time.UTC).Format(http.TimeFormat)}
	//req.headers["Date"] = []string{"Thu, 25 Jun 2015 06:29:40 GMT"}

	api.sign(req)
	return nil
}

func (api *OssApi) baseUrl() string {
	var protocol = "http"
	if api.secure {
		protocol = "https"
	}
	return fmt.Sprintf("%s://%s.%s.aliyuncs.com", protocol, api.bucket, api.region)
}

// this method returns the http response body
// and it's caller to close this Reader
func (api *OssApi) rawQuery(req *request) (*http.Response, error) {
	err := api.prepare(req)
	if err != nil {
		return nil, err
	}
	hresp, err := api.run(req)
	if err != nil {
		return nil, err
	}
	return hresp, nil
}

// this method dumps the http xml response to the resp object
// the http respons body is closed and can't use anymore
func (api *OssApi) query(req *request, resp interface{}) error {
	hresp, err := api.rawQuery(req)
	if err != nil {
		return err
	}
	if resp != nil {
		err = xml.NewDecoder(hresp.Body).Decode(resp)
	}
	defer hresp.Body.Close()
	return err
}

func (req *request) url() (*url.URL, error) {
	u, err := url.Parse(req.baseurl)
	if err != nil {
		return nil, fmt.Errorf("bad OSS endpoint URL %q: %v", req.baseurl, err)
	}
	u.RawQuery = req.params.Encode()
	u.Path = "/" + req.object
	return u, nil
}

// run sends req and returns the http response from the server.
func (api *OssApi) run(req *request) (*http.Response, error) {

	u, err := req.url()
	if err != nil {
		return nil, err
	}

	hreq := &http.Request{
		URL:        u,
		Method:     req.method,
		ProtoMajor: 1,
		ProtoMinor: 1,
		Close:      true,
		Header:     req.headers,
	}

	if v, ok := req.headers["Content-Length"]; ok {
		hreq.ContentLength, _ = strconv.ParseInt(v[0], 10, 64)
		delete(req.headers, "Content-Length")
	}

	if req.payload != nil {
		hreq.Body = ioutil.NopCloser(bytes.NewReader(req.payload))
	}
	if log.GetLevel() == log.DebugLevel {
		dump, _ := httputil.DumpRequestOut(hreq, false)
		log.Debugf("request } -> %s\n", dump)

	}

	hresp, err := http.DefaultClient.Do(hreq)
	if err != nil {
		return nil, err
	}
	if log.GetLevel() == log.DebugLevel {
		dump, _ := httputil.DumpResponse(hresp, false)
		log.Debugf("response } -> %s\n", dump)

	}

	if hresp.StatusCode < 200 || hresp.StatusCode >= 300 {
		return nil, buildError(hresp)
	}
	return hresp, err
}

// Error represents an error in an operation with OSS.
type Error struct {
	StatusCode int    // HTTP status code (200, 403, ...)
	Code       string // Oss error code ("UnsupportedOperation", ...)
	Message    string // The human-oriented error message
	BucketName string
	RequestId  string
	HostId     string
}

func (e *Error) Error() string {
	return e.Message
}
func buildError(r *http.Response) error {

	err := Error{}
	// TODO return error if Unmarshal fails?
	xml.NewDecoder(r.Body).Decode(&err)
	defer r.Body.Close()
	err.StatusCode = r.StatusCode
	if err.Message == "" {
		err.Message = r.Status
	}

	log.Debugf("err: %#v\n", err)

	return &err
}
