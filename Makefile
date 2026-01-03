.PHONY: test test-race lint \
	redis-start redis-stop test-redis \
	valkey-start valkey-stop test-valkey \
	dragonfly-start dragonfly-stop test-dragonfly

GO ?= go
GOCACHE ?= $(CURDIR)/.gocache
PKGS ?= ./...
GOLANGCI_LINT ?= golangci-lint
DOCKER ?= docker

REDIS_IMAGE ?= redis:7-alpine
VALKEY_IMAGE ?= valkey/valkey:8-alpine
DRAGONFLY_IMAGE ?= docker.dragonflydb.io/dragonflydb/dragonfly:latest
CONTAINER_NAME_PREFIX ?= senna-test

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

# Redis targets
redis-start:
	@$(DOCKER) run -d --name $(CONTAINER_NAME_PREFIX)-redis -p 6379:6379 $(REDIS_IMAGE) >/dev/null
	@echo "Redis started on port 6379"

redis-stop:
	@$(DOCKER) rm -f $(CONTAINER_NAME_PREFIX)-redis >/dev/null 2>&1 || true
	@echo "Redis stopped"

test-redis: redis-stop redis-start
	@sleep 1
	GOCACHE=$(GOCACHE) REDIS_ADDR=localhost:6379 $(GO) test -race $(PKGS); \
	status=$$?; \
	$(MAKE) redis-stop; \
	exit $$status

# Valkey targets
valkey-start:
	@$(DOCKER) run -d --name $(CONTAINER_NAME_PREFIX)-valkey -p 6379:6379 $(VALKEY_IMAGE) >/dev/null
	@echo "Valkey started on port 6379"

valkey-stop:
	@$(DOCKER) rm -f $(CONTAINER_NAME_PREFIX)-valkey >/dev/null 2>&1 || true
	@echo "Valkey stopped"

test-valkey: valkey-stop valkey-start
	@sleep 1
	GOCACHE=$(GOCACHE) REDIS_ADDR=localhost:6379 $(GO) test -race $(PKGS); \
	status=$$?; \
	$(MAKE) valkey-stop; \
	exit $$status

# DragonflyDB targets
dragonfly-start:
	@$(DOCKER) run -d --name $(CONTAINER_NAME_PREFIX)-dragonfly -p 6379:6379 $(DRAGONFLY_IMAGE) >/dev/null
	@echo "DragonflyDB started on port 6379"

dragonfly-stop:
	@$(DOCKER) rm -f $(CONTAINER_NAME_PREFIX)-dragonfly >/dev/null 2>&1 || true
	@echo "DragonflyDB stopped"

test-dragonfly: dragonfly-stop dragonfly-start
	@sleep 2
	GOCACHE=$(GOCACHE) REDIS_ADDR=localhost:6379 $(GO) test -race $(PKGS); \
	status=$$?; \
	$(MAKE) dragonfly-stop; \
	exit $$status
