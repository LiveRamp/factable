FROM liveramp/gazette AS base

FROM base AS build

ARG DEP_VERSION=v0.5.0
ARG IMPORT_PATH=github.com/LiveRamp/factable

RUN curl -fsSL -o /usr/local/bin/dep \
    https://github.com/golang/dep/releases/download/${DEP_VERSION}/dep-linux-amd64 \
 && chmod +x /usr/local/bin/dep

COPY Gopkg.toml Gopkg.lock /go/src/${IMPORT_PATH}/
RUN cd /go/src/${IMPORT_PATH}/ \
 && dep ensure -vendor-only \
 && rm -rf "$GOPATH/pkg/dep"

# Copy, install, and test library and main packages.
COPY pkg /go/src/${IMPORT_PATH}/pkg
RUN go install -tags rocksdb -race ${IMPORT_PATH}/pkg/...
RUN go test    -tags rocksdb -race ${IMPORT_PATH}/pkg/...
RUN go install -tags rocksdb       ${IMPORT_PATH}/pkg/...

COPY cmd /go/src/${IMPORT_PATH}/cmd
RUN go install -tags rocksdb ${IMPORT_PATH}/cmd/...
RUN go test    -tags rocksdb ${IMPORT_PATH}/cmd/...

# Stage 4: Pluck factable binaries & plugins onto base.
FROM base as factable

COPY --from=build \
    /go/bin/vtable \
    /go/bin/factctl \
    /go/bin/quotes-extractor \
    /go/bin/quotes-publisher \
    /go/bin/
