# Multi-stage build for the VaultFS CLI.
FROM golang:1.26 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/vaultfs ./cmd/vaultfs

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/vaultfs /usr/local/bin/vaultfs
ENTRYPOINT ["/usr/local/bin/vaultfs"]
