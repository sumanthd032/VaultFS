.PHONY: test lint build proto certs dev docker-build clean

# ----------------------------------------------
# Core targets
# ----------------------------------------------

test:
	go test -race ./...

GOLANGCI_LINT ?= $(shell go env GOPATH)/bin/golangci-lint

lint:
	$(GOLANGCI_LINT) run ./...

build:
	@mkdir -p bin
	go build -o bin/master ./cmd/master
	go build -o bin/chunkserver ./cmd/chunkserver
	go build -o bin/vaultfs ./cmd/vaultfs

# ----------------------------------------------
# Proto generation (requires buf)
# ----------------------------------------------

BUF ?= $(shell go env GOPATH)/bin/buf

proto:
	@test -x "$(BUF)" || \
		(echo "buf not installed: go install github.com/bufbuild/buf/cmd/buf@latest" && exit 1)
	PATH="$(shell go env GOPATH)/bin:$$PATH" $(BUF) lint
	PATH="$(shell go env GOPATH)/bin:$$PATH" $(BUF) generate

# ----------------------------------------------
# Local dev TLS certs (implemented in Step 5)
# ----------------------------------------------

certs:
	go run ./cmd/gen-certs deploy/certs

# ----------------------------------------------
# Local cluster via Docker Compose (Step 5)
# ----------------------------------------------

dev:
	docker compose -f deploy/docker-compose.yml up --build

# ----------------------------------------------
# Docker image builds (Step 5)
# ----------------------------------------------

docker-build:
	docker build -t vaultfs-master:latest    -f cmd/master/Dockerfile .
	docker build -t vaultfs-chunkserver:latest -f cmd/chunkserver/Dockerfile .
	docker build -t vaultfs-cli:latest       -f cmd/vaultfs/Dockerfile .

# ----------------------------------------------
# Cleanup
# ----------------------------------------------

clean:
	rm -rf bin/
