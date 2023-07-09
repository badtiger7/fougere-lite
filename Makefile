.DEFAULT_GOAL = help

# Project variables
NAME          := fougere-lite

# Build variables
COMMIT_HASH := $(shell git rev-parse --short HEAD 2>/dev/null)
VERSION     := $(shell git describe --tags --exact-match 2>/dev/null || git describe --tags 2>/dev/null || echo "v0.0.0-$(COMMIT_HASH)")
BUILD_DATE  := $(shell date +%FT%T%z)

# Go variables
LDFLAGS     := -X main.version=$(VERSION) -X main.commitHash=${COMMIT_HASH} -X main.buildDate=${BUILD_DATE}

.PHONY: clean
clean: log-clean ## Clean builds
	rm -f $(NAME)

.PHONY: test
test: log-test ## Run tests
	@mkdir -p coverage
	@go run github.com/onsi/ginkgo/v2/ginkgo -r --cover --fail-fast --output-dir=coverage --coverprofile=all.coverprofile

.PHONY: build
build: GOOS   = $(shell go env GOOS)
build: GOARCH = $(shell go env GOARCH)
build: clean log-build ## Build binary for current OS/ARCH
	@ echo OS: ${GOOS} ARCH: ${GOARCH}
	@ CGO_ENABLED=0 GOOS=${GOOS} GOARCH=${GOARCH} go build -o fougere-lite -ldflags "$(LDFLAGS)" ./cmd/

.PHONY: help
help:
	@ grep -h -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

log-%:
	@ grep -h -E '^$*:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m==> %s\033[0m\n", $$2}'

