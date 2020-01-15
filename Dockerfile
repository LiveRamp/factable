FROM gazette/ci-builder AS base

WORKDIR /workspace
COPY go.mod go.sum ./
COPY pkg pkg
RUN go mod download

# Install RocksDB
ARG ROCKSDB_VERSION=5.17.2

RUN mkdir -p rocksdb-v${ROCKSDB_VERSION} && \
    curl -L -o tmp.tgz https://github.com/facebook/rocksdb/archive/v${ROCKSDB_VERSION}.tar.gz && \
    tar xzf tmp.tgz -C rocksdb-v${ROCKSDB_VERSION} --strip-components=1 && \
    rm tmp.tgz && \
    USE_SSE=1 DEBUG_LEVEL=0 USE_RTTI=1 \
    	make -C rocksdb-v$ROCKSDB_VERSION shared_lib -j$(nproc)

RUN mv rocksdb-v$ROCKSDB_VERSION/librocksdb.so* /usr/local/lib && ldconfig

ENV CGO_CFLAGS=-I/workspace/rocksdb-v5.17.2/include
ENV CGO_CPPFLAGS=-I/workspace/rocksdb-v5.17.2/include
ENV CGO_LDFLAGS="-L/workspace/rocksdb-v5.17.2 -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"
ENV LD_LIBRARY_PATH=/workspace/rocksdb-v5.17.2

# Install gorocksdb
RUN CGO_CFLAGS="-I/workspace/rocksdb-v5.17.2/include" \
    CGO_LDFLAGS="-L/workspace/rocksdb-v5.17.2 -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd" \
    go get github.com/tecbot/gorocksdb

# Copy, install, and test library and main packages.
RUN CGO_CFLAGS="-I/workspace/rocksdb-v5.17.2/include" \
    CGO_LDFLAGS="-L/workspace/rocksdb-v5.17.2 -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd" go install -tags rocksdb -race ./pkg/...
RUN go test -tags rocksdb -race ./pkg/...
RUN go install -tags rocksdb ./pkg/...

COPY cmd cmd
RUN go install -tags rocksdb ./cmd/...
RUN go test -tags rocksdb ./cmd/...

# Stage 4: Pluck factable binaries & plugins onto base.
FROM base as factable

COPY --from=base \
    /go/bin/vtable \
    /go/bin/factctl \
    /go/bin/quotes-extractor \
    /go/bin/quotes-publisher \
    /go/bin/
