BINARY   := cvmfs-gc
CMD      := ./cmd/cvmfs-gc
GOFLAGS  ?=

# Ensure go.mod/go.sum are up to date.
.PHONY: tidy
tidy:
	go mod tidy

# Default: build for the host platform.
.PHONY: build
build: tidy
	CGO_ENABLED=0 go build $(GOFLAGS) -o $(BINARY) $(CMD)

# Cross-compile a static linux/amd64 binary.
# No cgo or extra toolchain needed — SQLite is pure Go.
.PHONY: linux
linux: tidy
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
		go build $(GOFLAGS) -o $(BINARY)-linux-amd64 $(CMD)

.PHONY: test
test:
	go test ./... -v -count=1

.PHONY: clean
clean:
	rm -f $(BINARY) $(BINARY)-linux-amd64

.PHONY: help
help:
	@echo "Targets:"
	@echo "  build   Build for the host platform (default)"
	@echo "  linux   Cross-compile static linux/amd64 binary"
	@echo "  tidy    Run go mod tidy"
	@echo "  test    Run all tests"
	@echo "  clean   Remove built binaries"
