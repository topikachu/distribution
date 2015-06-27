// Package OSS provides a storagedriver.StorageDriver implementation to
// store blobs in Ali OSS cloud storage.
//
// This package leverages the AdRoll/goamz client library for interfacing with
// OSS.
//
// Because OSS is a key, value store the Stat call does not support last modification
// time for directories (directories are an abstraction for key, value stores)
//
// Keep in mind that OSS guarantees only eventual consistency, so do not assume
// that a successful write will mean immediate access to the data written (although
// in most regions a new object put has guaranteed read after write). The only true
// guarantee is that once you call Stat and receive a certain file size, that much of
// the file is already accessible.
package oss

import (
	"bytes"
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
	osssdk "github.com/topikachu/oss"
	"io"
	"io/ioutil"
	"reflect"
	"strconv"
	"strings"
)

const driverName = "oss"

// minChunkSize defines the minimum multipart upload chunk size
// OSS API requires multipart upload chunks to be at least 5MB
const minChunkSize = 100 << 10

const defaultChunkSize = 5 << 20

// listMax is the largest amount of objects you can request from OSS in a list call
const listMax = 1000

//DriverParameters A struct that encapsulates all of the driver parameters after all values have been set
type DriverParameters struct {
	AccessKeyId     string
	AccessKeySecret string
	Bucket          string
	Region          string
	RootDirectory   string
	ChunkSize       int64
}

func init() {
	factory.Register(driverName, &ossDriverFactory{})
}

// ossDriverFactory implements the factory.StorageDriverFactory interface
type ossDriverFactory struct{}

func (factory *ossDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

type driver struct {
	Api           *osssdk.OssApi
	RootDirectory string
	ChunkSize     int64
}

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by Ali OSS
// Objects are stored at absolute keys in the provided bucket.
type Driver struct {
	baseEmbed
}

// FromParameters constructs a new Driver with a given parameters map
// Required parameters:
// - accessKeyId
// - accessKeySecret
// - region
// - bucket

func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	// Providing no values for these is valid in case the user is authenticating
	// with an IAM on an ec2 instance (in which case the instance credentials will
	// be summoned when GetAuth is called)
	accessKeyId, ok := parameters["accesskey"]
	if !ok {
		accessKeyId = ""
	}
	accessKeySecret, ok := parameters["secretkey"]
	if !ok {
		accessKeySecret = ""
	}

	region, ok := parameters["region"]
	if !ok || fmt.Sprint(region) == "" {
		region = "oss"
	}

	bucket, ok := parameters["bucket"]
	if !ok || fmt.Sprint(bucket) == "" {
		return nil, fmt.Errorf("No bucket parameter provided")
	}

	rootDirectory, ok := parameters["rootdirectory"]
	if !ok {
		rootDirectory = ""
	}

	chunkSize := int64(defaultChunkSize)
	chunkSizeParam, ok := parameters["chunksize"]
	if ok {
		switch v := chunkSizeParam.(type) {
		case string:
			vv, err := strconv.ParseInt(v, 0, 64)
			if err != nil {
				log.Warnf("chunksize parameter must be an integer, %v invalid, use default value %d", chunkSizeParam, defaultChunkSize)
			} else {
				chunkSize = vv
			}
		case int64:
			chunkSize = v
		case int, uint, int32, uint32, uint64:
			chunkSize = reflect.ValueOf(v).Convert(reflect.TypeOf(chunkSize)).Int()
		default:
			log.Warnf("invalid valud for chunksize: %#v, use default value %d", chunkSizeParam, defaultChunkSize)
		}

		if chunkSize < minChunkSize {
			log.Warnf("The chunksize %#v parameter should be a number that is larger than or equal to %d, use the default value %d", chunkSize, minChunkSize, defaultChunkSize)
			chunkSize = defaultChunkSize
		}
	}

	params := DriverParameters{
		fmt.Sprint(accessKeyId),
		fmt.Sprint(accessKeySecret),
		fmt.Sprint(bucket),
		fmt.Sprint(region),
		fmt.Sprint(rootDirectory),
		chunkSize,
	}

	return New(params)
}

// New constructs a new Driver with the given AWS credentials, region, encryption flag, and
// bucketName
func New(params DriverParameters) (*Driver, error) {
	api := osssdk.New(params.Region, params.AccessKeyId, params.AccessKeySecret, params.Bucket)
	d := &driver{
		Api:           api,
		RootDirectory: strings.Trim(params.RootDirectory, "/"),
		ChunkSize:     params.ChunkSize,
	}

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}, nil
}

// Implement the storagedriver.StorageDriver interface

func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	contents, err := d.Api.GetObject(d.ossPath(path))
	if err != nil {
		return nil, parseError(path, err)
	}
	return contents, nil
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
	return parseError(path, d.Api.PutObject(d.ossPath(path), contents, d.getContentType()))
}

// ReadStream retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) ReadStream(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	contents, status, err := d.Api.GetObjectRange(d.ossPath(path), offset, -1)
	if err != nil {
		return nil, parseError(path, err)
	}
	if offset != 0 && status != 206 {
		return ioutil.NopCloser(bytes.NewReader(nil)), nil
	}
	return contents, nil
}

// WriteStream stores the contents of the provided io.Reader at a
// location designated by the given path. The driver will know it has
// received the full contents when the reader returns io.EOF. The number
// of successfully READ bytes will be returned, even if an error is
// returned. May be used to resume writing a stream by providing a nonzero
// offset. Offsets past the current size will write from the position
// beyond the end of the file.
func (d *driver) WriteStream(ctx context.Context, path string, offset int64, reader io.Reader) (totalRead int64, err error) {
	partNumber := 1
	var uploadContext *osssdk.UploadContext
	if offset != 0 {
		header, err := d.Api.GetObjectMetadata(d.ossPath(path))
		if err != nil {
			return 0, parseError(path, err)
		}

		length, err := header.GetContentLength()
		if length < offset {
			return 0, errors.New("Can't resume the upload. No enough existing parts")
		}
		uploadContext, err = d.Api.InitMultipartUpload(d.ossPath(path), d.getContentType())
		start, end := int64(0), int64(d.ChunkSize-1)

		for {
			if start >= offset {
				break
			}
			if end >= offset {
				end = offset - 1
			}
			_, err = d.Api.UploadCopyMultipart(uploadContext, "", d.ossPath(path), start, end, partNumber)
			if err != nil {
				return 0, err
			}
			partNumber++
			start += d.ChunkSize
			end += d.ChunkSize
		}

	} else {
		uploadContext, err = d.Api.InitMultipartUpload(d.ossPath(path), d.getContentType())
		if err != nil {
			return 0, err
		}
	}

	buffer := make([]byte, d.ChunkSize)
	totalRead = 0
	for {
		n, err := reader.Read(buffer)
		if err != nil && err != io.EOF {
			return totalRead, parseError(path, err)
		}
		if n == 0 {
			break
		}
		err = d.Api.UploadMultipart(uploadContext, buffer[:n], partNumber)
		if err != nil {
			return 0, parseError(path, err)
		}
		partNumber++
		totalRead += int64(n)
	}
	err = d.Api.CompleteMultipart(uploadContext)
	if err != nil {
		return totalRead, parseError(path, err)
	}

	return totalRead, nil
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	files, folder, _, err := d.Api.ListFiles(d.ossPath(path), "", "", 1)

	if err != nil {
		return nil, err
	}

	fi := storagedriver.FileInfoFields{
		Path: path,
	}

	if len(files) == 1 {
		objectFile := files[0]
		if objectFile != d.ossPath(path) {
			fi.IsDir = true
		} else {
			fi.IsDir = false
			headers, err := d.Api.GetObjectMetadata(objectFile)
			if err != nil {
				return nil, err
			}
			fi.Size, err = headers.GetContentLength()
			if err != nil {
				return nil, err
			}
			fi.ModTime, err = headers.GetLastModified()
			if err != nil {
				return nil, err
			}

		}
	} else if len(folder) == 1 {
		fi.IsDir = true
	} else {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}

	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// List returns a list of the objects that are direct descendants of the given path.
func (d *driver) List(ctx context.Context, path string) ([]string, error) {
	if !strings.HasSuffix(path, "/") {
		path = path + "/"
	}

	// This is to cover for the cases when the rootDirectory of the driver is either "" or "/".
	// In those cases, there is no root prefix to replace and we must actually add a "/" to all
	// results in order to keep them as valid paths as recognized by storagedriver.PathRegexp
	prefix := ""
	if d.ossPath("") == "" {
		prefix = "/"
	}

	files := []string{}
	marker := ""
	for {
		fileNames, folderNames, marker, err := d.Api.ListFiles(d.ossPath(path), "/", marker, listMax)
		if err != nil {
			return nil, err
		}

		//we need to normalize the "/" to satisfy docker
		for _, fileName := range fileNames {
			files = append(files, "/"+fileName)
		}
		for _, folderName := range folderNames {
			files = append(files, "/"+strings.TrimRight(folderName, "/"))
		}
		if marker == "" {
			break
		}
	}

	for i, name := range files {
		files[i] = strings.Replace(name, d.ossPath(""), prefix, 1)
	}

	return files, nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	/* This is terrible, but aws doesn't have an actual move. */
	err := d.Api.Copy("", d.ossPath(sourcePath), d.ossPath(destPath), d.getContentType(), d.ChunkSize)
	if err != nil {
		return parseError(sourcePath, err)
	}

	return d.Api.Delete(d.ossPath(sourcePath))
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, path string) error {
	marker := ""
	for {
		files, _, marker, err := d.Api.ListFiles(d.ossPath(path), "", marker, listMax)
		if err != nil || len(files) == 0 {
			return storagedriver.PathNotFoundError{Path: path}
		}
		if err != nil {
			return err
		}
		d.Api.Delete(files...)
		if marker == "" {
			break
		}
	}
	return nil
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	return "", storagedriver.ErrUnsupportedMethod
}

func (d *driver) ossPath(path string) string {
	return d.RootDirectory + "/" + strings.TrimLeft(path, "/")
}

func parseError(path string, err error) error {
	if ossErr, ok := err.(*osssdk.Error); ok && ossErr.StatusCode == 404 {
		return storagedriver.PathNotFoundError{Path: path}
	}

	return err
}

func (d *driver) getContentType() string {
	return "application/octet-stream"
}
