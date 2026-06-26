# Multi-stage build for the VaultFS chunk server.
FROM golang:1.26 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/chunkserver ./cmd/chunkserver

# Root distroless so the process can write to the mounted chunk-data volume.
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/chunkserver /usr/local/bin/chunkserver
EXPOSE 9100 9101
ENTRYPOINT ["/usr/local/bin/chunkserver"]
