# Contributing to VaultFS

Thanks for your interest in VaultFS. This guide covers how to build the project
locally, the conventions the codebase follows, and what a pull request needs to
pass review.

## Prerequisites

- Go 1.26 or newer
- `make`
- `openssl` (for generating local development certificates)
- Docker and Docker Compose (for the local cluster)
- `golangci-lint` v2 (`go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest`)

## Local development

```bash
make build         # build all binaries
make test          # run the full test suite with the race detector
make lint          # golangci-lint, zero issues required
make certs         # generate the development mTLS PKI into deploy/certs
make dev           # bring up the full cluster via docker compose
```

The CLI talks to the cluster on `localhost:9000`:

```bash
vaultfs --cert deploy/certs/client.crt --key deploy/certs/client.key \
        --ca deploy/certs/ca.crt put ./file.txt /file.txt
```

## Project layout

- `cmd/` thin entry points: flag parsing and wiring only
- `internal/` library code, not importable outside this module
- `pkg/` the stable public API, importable by other Go programs
- `proto/` gRPC service definitions and generated code
- `deploy/` docker compose, Kubernetes manifests, and observability config
- `docs/` architecture, tech stack, and conventions

## Coding conventions

The full set lives in `docs/CONVENTIONS.md`. The essentials:

- One responsibility per package; small interfaces defined at the consumer side.
- Every I/O or blocking function takes `context.Context` as its first argument.
- Wrap errors with context (`fmt.Errorf("pkg: doing x: %w", err)`); no `panic`,
  `log.Fatal`, or `os.Exit` outside `cmd/*/main.go`.
- No global state and no `init()` functions; pass dependencies explicitly.
- Structured logging through `log/slog`, never `fmt.Println` in library code.
- Tests are table-driven and use `t.TempDir()` for file-based cases; cover error
  paths, not just the happy path.

## Commit messages

Conventional Commits, enforced by review:

```
<type>(<scope>): <subject>
```

- Types: `feat`, `fix`, `test`, `refactor`, `docs`, `chore`, `perf`
- Subject: imperative mood, lowercase, no trailing period, at most 72 characters
- Body: explain the why, not the what
- Keep each commit a logical unit that leaves the tests passing

## Pull request checklist

Before opening a PR, confirm:

- [ ] `make test` passes (race detector clean)
- [ ] `make lint` reports zero issues
- [ ] New and changed exported symbols have doc comments
- [ ] New behavior has tests, including error paths
- [ ] Commits follow the Conventional Commits format above
- [ ] Documentation is updated when behavior or interfaces change

CI runs the same test and lint gates on every push and pull request, and
publishes container images from `main`.
