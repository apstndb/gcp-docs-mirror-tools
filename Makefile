BINARY_NAME=gcp-docs-mirror
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT=$(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME=$(shell date -u +'%Y-%m-%dT%H:%M:%SZ')

LDFLAGS=-ldflags "-X main.Version=$(VERSION) -X main.Commit=$(COMMIT) -X main.BuildTime=$(BUILD_TIME)"

.PHONY: all
all: build

.PHONY: build
build:
	go build $(LDFLAGS) -o $(BINARY_NAME) .

.PHONY: install
install:
	go install $(LDFLAGS) .

.PHONY: lint
lint:
	golangci-lint run

.PHONY: test
test:
	go test -v ./...

.PHONY: clean
clean:
	go clean
	rm -f $(BINARY_NAME)
	rm -f gcp-docs-mirror-tools

.PHONY: help
help:
	@echo "GCP Docs Mirror Tools - Makefile"
	@echo "Usage:"
	@echo "  make build   - Build the standardized binary ($(BINARY_NAME))"
	@echo "  make install - Install the binary to \$$GOPATH/bin"
	@echo "  make lint    - Run golangci-lint"
	@echo "  make test    - Run all unit tests"
	@echo "  make clean   - Remove binaries and build artifacts"
