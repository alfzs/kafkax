GO=go
TOOLS_DIR=.tools

$(TOOLS_DIR)/golangci-lint:
	GOBIN=$(PWD)/$(TOOLS_DIR) $(GO) install -C tools github.com/golangci/golangci-lint/v2/cmd/golangci-lint

$(TOOLS_DIR)/govulncheck:
	GOBIN=$(PWD)/$(TOOLS_DIR) $(GO) install -C tools golang.org/x/vuln/cmd/govulncheck

.PHONY: all test test-cover lint lint-fix fmt vet audit tidy clean help

all: fmt vet lint test

## test: Run all tests with the race detector
test:
	$(GO) test -race -count=1 ./...

## test-cover: Run tests with coverage report
test-cover:
	$(GO) test -race -count=1 -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html

## lint: Run golangci-lint
lint: $(TOOLS_DIR)/golangci-lint
	$(TOOLS_DIR)/golangci-lint run ./...

## lint-fix: Run golangci-lint and auto-fix issues
lint-fix: $(TOOLS_DIR)/golangci-lint
	$(TOOLS_DIR)/golangci-lint run --fix ./...

## fmt: Check gofmt formatting (read-only; `make lint-fix` rewrites files)
# gofmt -l, а не `go fmt`: последний переписывает файлы на месте, так что цель,
# заявленная как проверка, молча меняла рабочее дерево — и в CI «проваливалась»
# уже после того, как исправила причину провала.
fmt:
	@out="$$($$($(GO) env GOROOT)/bin/gofmt -l .)"; \
	test -z "$$out" || (echo "gofmt found issues:"; echo "$$out"; exit 1)

## vet: Run go vet
vet:
	$(GO) vet ./...

## audit: Check dependencies for known vulnerabilities
audit: $(TOOLS_DIR)/govulncheck
	$(TOOLS_DIR)/govulncheck ./...

## tidy: Tidy go.mod/go.sum
tidy:
	$(GO) mod tidy

## clean: Remove installed tools
clean:
	rm -rf $(TOOLS_DIR)

## help: Show this help message
help:
	@echo "Usage: make [target]"
	@sed -n 's/^##//p' $(MAKEFILE_LIST) | column -t -s ':' | sed -e 's/^/ /'
