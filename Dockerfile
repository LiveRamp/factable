####################################################
# This Dockerfile is suitable for building ping-pong
#FROM gazette/ci-builder
#
#ENV GO111MODULE=on
#
#RUN go get go.gazette.dev/ping-pong
#
#RUN cd $GOPATH/pkg/mod/go.gazette.dev@v0.0.0-20191204015027-4ddaca970ab8 && \
#    make go-install
####################################################

FROM gazette/ci-builder as base

ENV GO111MODULE=on

RUN go get github.com/LiveRamp/factable@v0.1.0

FROM base as baz

## Copy, install, and test library and main packages.
#RUN CGO_CFLAGS="-I/workspace/rocksdb-v5.17.2/include" \
#    CGO_LDFLAGS="-L/workspace/rocksdb-v5.17.2 -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd" go install -tags rocksdb -race ./pkg/...
#RUN go test -tags rocksdb -race ./pkg/...
#RUN go install -tags rocksdb ./pkg/...
#
#COPY cmd cmd
#RUN go install -tags rocksdb ./cmd/...
#RUN go test -tags rocksdb ./cmd/...
#
## Stage 4: Pluck factable binaries & plugins onto base.
#FROM base as factable
#
#COPY --from=base \
#    /go/bin/vtable \
#    /go/bin/factctl \
#    /go/bin/quotes-extractor \
#    /go/bin/quotes-publisher \
#    /go/bin/
