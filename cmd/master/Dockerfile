# Multi-stage build for the VaultFS master.
# Stage 1 compiles a static binary; stage 2 ships only that binary on a minimal
# distroless base (includes CA roots and runs as nonroot).
FROM golang:1.26 AS build
WORKDIR /src

# Cache module downloads.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/master ./cmd/master

# Root distroless so the process can write to the mounted metadata volume.
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/master /usr/local/bin/master
EXPOSE 9000 9200
ENTRYPOINT ["/usr/local/bin/master"]
