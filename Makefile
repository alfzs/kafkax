GO=go
TOOLS_DIR=.tools
TOOLS_DEPS=tools/go.mod tools/go.sum

# Бинарники линтеров зависят от манифеста tools/ по mtime: подъём версии в
# tools/go.mod (или tools/go.sum) делает установленный бинарник устаревшим, и
# make переустанавливает его сам. Без этих предпосылок цель считалась
# выполненной по факту существования файла — локально `make lint` оставался
# зелёным на старой версии, а CI с пустым .tools/ ставил новую и краснел.
$(TOOLS_DIR)/golangci-lint: $(TOOLS_DEPS)
	GOBIN=$(PWD)/$(TOOLS_DIR) $(GO) install -C tools github.com/golangci/golangci-lint/v2/cmd/golangci-lint

$(TOOLS_DIR)/govulncheck: $(TOOLS_DEPS)
	GOBIN=$(PWD)/$(TOOLS_DIR) $(GO) install -C tools golang.org/x/vuln/cmd/govulncheck

COVERAGE_FILES=coverage.out coverage.html

.PHONY: all test test-cover lint lint-fix fmt vet audit tidy clean help

all: fmt vet lint test

## test: Run all tests with the race detector
test:
	$(GO) test -race -count=1 ./...

## test-cover: Run tests with coverage report (artifacts removed by `make clean`)
test-cover:
	$(GO) test -race -count=1 -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html

## test-integration: Run integration tests against a real broker (requires Docker)
#
# Отдельный модуль, поэтому и отдельная цель: `go test ./...` в корне его не
# видит и Docker не требует. KAFKAX_INTEGRATION=required превращает
# недоступность Docker из пропуска в отказ — на машине разработчика пропуск
# уместен, в конвейере он означал бы зелёный прогон, ничего не проверивший.
test-integration:
	KAFKAX_INTEGRATION=required $(GO) test -C integration -race -count=1 ./...

## lint: Run golangci-lint
#
# Модули перечислены поимённо: golangci-lint работает в границах одного модуля,
# и `run ./...` из корня молча пропускал бы integration/ и tools/ — вместе с
# ними без присмотра оставался весь интеграционный набор.
lint: $(TOOLS_DIR)/golangci-lint
	$(TOOLS_DIR)/golangci-lint run ./...
	cd integration && $(PWD)/$(TOOLS_DIR)/golangci-lint run -c $(PWD)/.golangci.yml ./...

## lint-fix: Run golangci-lint and auto-fix issues
lint-fix: $(TOOLS_DIR)/golangci-lint
	$(TOOLS_DIR)/golangci-lint run --fix ./...
	cd integration && $(PWD)/$(TOOLS_DIR)/golangci-lint run --fix -c $(PWD)/.golangci.yml ./...

## fmt: Check gofmt formatting (read-only; `make lint-fix` rewrites files)
# gofmt -l, а не `go fmt`: последний переписывает файлы на месте, так что цель,
# заявленная как проверка, молча меняла рабочее дерево — и в CI «проваливалась»
# уже после того, как исправила причину провала.
#
# Список файлов берётся из git, а не обходом каталога: `gofmt -l .` спускается и
# в .claude/worktrees, где лежат рабочие деревья агентов. В CI их нет, и цель
# там зелёная, а локально она краснела на чужом незакоммиченном коде.
fmt:
	@out="$$(git ls-files '*.go' | xargs $$($(GO) env GOROOT)/bin/gofmt -l)"; \
	test -z "$$out" || (echo "gofmt found issues:"; echo "$$out"; exit 1)

## vet: Run go vet
vet:
	$(GO) vet ./...
	$(GO) vet -C integration ./...

## audit: Check dependencies for known vulnerabilities
audit: $(TOOLS_DIR)/govulncheck
	$(TOOLS_DIR)/govulncheck ./...

## tidy: Tidy go.mod/go.sum
tidy:
	$(GO) mod tidy

## clean: Remove installed tools and coverage artifacts
clean:
	rm -rf $(TOOLS_DIR)
	rm -f $(COVERAGE_FILES)

## help: Show this help message
help:
	@echo "Usage: make [target]"
	@sed -n 's/^##//p' $(MAKEFILE_LIST) | column -t -s ':' | sed -e 's/^/ /'
