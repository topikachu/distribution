package oss

import (
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/testsuites"
	"gopkg.in/check.v1"
	"io/ioutil"
	"os"
	"strconv"
	"testing"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { check.TestingT(t) }

type OssDriverConstructor func() (*Driver, error)

func init() {
	accessKey := os.Getenv("OSS_ACCESS_KEY")
	secretKey := os.Getenv("OSS_SECRET_KEY")
	bucket := os.Getenv("OSS_BUCKET")
	region := os.Getenv("OSS_REGION")
	chunkSizeStr := os.Getenv("OSS_CHUNK_SIZE")
	chunkSize, err := strconv.ParseInt(chunkSizeStr, 0, 64)
	if err != nil {
		chunkSize = defaultChunkSize
	}
	//a tricky way to get a random file name
	root, err := ioutil.TempDir("", "driver-")
	if err != nil {
		panic(err)
	}
	defer os.Remove(root)

	ossDriverConstructor := func() (*Driver, error) {

		parameters := DriverParameters{
			accessKey,
			secretKey,
			bucket,
			region,
			root,
			chunkSize,
		}

		return New(parameters)
	}

	// Skip oss storage driver tests if environment variable parameters are not provided
	skipCheck := func() string {
		if accessKey == "" || secretKey == "" || region == "" || bucket == "" {
			return "Must set OSS_ACCESS_KEY, OSS_SECRET_KEY, OSS_REGION, OSS_BUCKET to run OSS tests"
		}
		return ""
	}

	driverConstructor := func() (storagedriver.StorageDriver, error) {
		return ossDriverConstructor()
	}

	testsuites.RegisterInProcessSuite(driverConstructor, skipCheck)

	RegisterOssDriverSuite(ossDriverConstructor, skipCheck)

}

func RegisterOssDriverSuite(ossDriverConstructor OssDriverConstructor, skipCheck testsuites.SkipCheck) {
	check.Suite(&OssDriverSuite{
		Constructor: ossDriverConstructor,
		SkipCheck:   skipCheck,
	})
}

type OssDriverSuite struct {
	Constructor OssDriverConstructor
	testsuites.SkipCheck
}

func (suite *OssDriverSuite) SetUpSuite(c *check.C) {
	if reason := suite.SkipCheck(); reason != "" {
		c.Skip(reason)
	}
}
