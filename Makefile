.PHONY: test test-race lint

GO ?= go
GOCACHE ?= $(CURDIR)/.gocache
PKGS ?= ./...
GOLANGCI_LINT ?= golangci-lint

test:
	GOCACHE=$(GOCACHE) $(GO) test $(PKGS)

test-race:
	GOCACHE=$(GOCACHE) $(GO) test -race $(PKGS)

lint:
	@command -v $(GOLANGCI_LINT) >/dev/null 2>&1 && { \
		echo "running $(GOLANGCI_LINT)"; \
		GOCACHE=$(GOCACHE) $(GOLANGCI_LINT) run ./...; \
	} || { \
		echo "$(GOLANGCI_LINT) not found; falling back to go vet"; \
		GOCACHE=$(GOCACHE) $(GO) vet $(PKGS); \
	}
