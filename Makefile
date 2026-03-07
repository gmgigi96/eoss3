GOCMD=go
GOBUILD=$(GOCMD) build

PLUGIN=eoss3.so
CLI=eoss3-cli
MODULE = github.com/versity/versitygw
BINARY_NAME = versitygw
TARGET_BIN = /usr/local/bin/$(BINARY_NAME) 

.PHONY: all
all: build

.PHONY: build
build: deps $(PLUGIN)

.PHONY: $(PLUGIN)
$(PLUGIN):
	@cp go.mod go.mod.bak
	@cp go.sum go.sum.bak
	@echo "Extracting exact dependency tree from $(TARGET_BIN)..."
	@go version -m $(TARGET_BIN) | grep 'dep' | awk '{print "go mod edit -require="$$2"@"$$3}' > sync_deps.sh
	@chmod +x sync_deps.sh
	@./sync_deps.sh
	@rm sync_deps.sh
	@go mod tidy
	@echo "Building plugin..."
	$(GOBUILD) -buildmode=plugin -o $(PLUGIN) plugin.go
	@mv go.mod.bak go.mod
	@mv go.sum.bak go.sum

.PHONY: deps
deps:
	$(eval DESIRED_VER=$(shell go list -f '{{.Version}}' -m $(MODULE)))
	$(eval CURRENT_VER=$(shell [ -f $(TARGET_BIN) ] && go version -m $(TARGET_BIN) | grep github.com/versity/versitygw | grep mod | awk '{print $$3}' || echo "none"))
	@if [ "$(DESIRED_VER)" = "$(CURRENT_VER)" ]; then \
		echo "$(BINARY_NAME) is already up to date ($(DESIRED_VER))."; \
	else \
		echo "Update required: Current($(CURRENT_VER)) -> Desired($(DESIRED_VER))"; \
		GOBIN=$(shell dirname $(TARGET_BIN)) go install $(MODULE)/cmd/$(BINARY_NAME)@$(DESIRED_VER); \
		restorecon $(TARGET_BIN); \
	fi

.PHONY: cli
cli:
	$(GOBUILD) -o $(CLI) cli/main.go

.PHONY: install
install: $(PLUGIN) cli
	@cp $(PLUGIN) /usr/local/lib/eoss3.so
	@cp $(CLI) /usr/local/bin/eoss3-cli

.PHONY: clean
clean:
	@rm -f $(PLUGIN)
	@rm -f $(CLI)
