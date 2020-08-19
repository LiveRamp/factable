FROM liveramp/gazette AS base

FROM base AS build

WORKDIR /workspace
COPY go.mod go.sum ./
COPY pkg pkg
RUN go mod download

# Copy, install, and test library and main packages.
RUN go install -tags rocksdb -race ./pkg/...
RUN go test -tags rocksdb -race ./pkg/...
RUN go install -tags rocksdb ./pkg/...

COPY cmd cmd
RUN go install -tags rocksdb ./cmd/...
RUN go test -tags rocksdb ./cmd/...

# Stage 4: Pluck factable binaries & plugins onto base.
FROM base as factable

COPY --from=build \
    /go/bin/vtable \
    /go/bin/factctl \
    /go/bin/quotes-backfill \
    /go/bin/quotes-extractor \
    /go/bin/quotes-publisher \
    /go/bin/
