FROM golang:1.4



ENV DISTRIBUTION_DIR /go/src/github.com/docker/distribution
ENV GOPATH $DISTRIBUTION_DIR/Godeps/_workspace:$GOPATH

WORKDIR $DISTRIBUTION_DIR
COPY . $DISTRIBUTION_DIR
RUN make PREFIX=/go clean binaries
WORKDIR $DISTRIBUTION_DIR/registry/storage/driver/oss



