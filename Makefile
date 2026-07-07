GO=go

.PHONY: all test test-cover benchmark lint lint-fix fmt vet audit tidy help

all: fmt vet lint test

## test: Run all tests with the race detector
test:
	$(GO) test -race -count=1 ./...

## test-cover: Run tests with coverage report
test-cover:
	$(GO) test -race -count=1 -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html

## benchmark: Run benchmarks
benchmark:
	$(GO) test -bench=. -benchmem ./...

## lint: Run golangci-lint
lint:
	$(GO) tool golangci-lint run ./...

## lint-fix: Run golangci-lint and auto-fix issues
lint-fix:
	$(GO) tool golangci-lint run --fix ./...

## fmt: Check gofmt formatting
fmt:
	@test -z "$$($(GO) fmt ./...)" || (echo "gofmt found issues" && exit 1)

## vet: Run go vet
vet:
	$(GO) vet ./...

## audit: Check dependencies for known vulnerabilities
audit:
	$(GO) tool govulncheck ./...

## tidy: Tidy go.mod/go.sum
tidy:
	$(GO) mod tidy

## help: Show this help message
help:
	@echo "Usage: make [target]"
	@sed -n 's/^##//p' $(MAKEFILE_LIST) | column -t -s ':' | sed -e 's/^/ /'
