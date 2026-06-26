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

proto:
	@which buf > /dev/null 2>&1 || \
		(echo "buf not installed: go install github.com/bufbuild/buf/cmd/buf@latest" && exit 1)
	buf generate

# ----------------------------------------------
# Local dev TLS certs (implemented in Step 5)
# ----------------------------------------------

certs:
	@which openssl > /dev/null 2>&1 || (echo "openssl not installed" && exit 1)
	@mkdir -p deploy/certs
	@echo "Generating cluster CA..."
	@openssl genrsa -out deploy/certs/ca.key 4096 2>/dev/null
	@openssl req -new -x509 -key deploy/certs/ca.key \
		-out deploy/certs/ca.crt -days 3650 \
		-subj "/CN=VaultFS-CA" 2>/dev/null
	@echo "Certs written to deploy/certs/ (gitignored)"

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
