.PHONY: test test-race lint

GO ?= go
GOCACHE ?= $(CURDIR)/.gocache
PKGS ?= ./...

test:
	GOCACHE=$(GOCACHE) $(GO) test $(PKGS)

test-race:
	GOCACHE=$(GOCACHE) $(GO) test -race $(PKGS)

lint:
	GOCACHE=$(GOCACHE) $(GO) vet $(PKGS)
