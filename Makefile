.PHONY: test test-race

GO ?= go
GOCACHE ?= $(CURDIR)/.gocache
PKGS ?= ./...

test:
	GOCACHE=$(GOCACHE) $(GO) test $(PKGS)

test-race:
	GOCACHE=$(GOCACHE) $(GO) test -race $(PKGS)
