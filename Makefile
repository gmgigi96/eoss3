GOCMD=go
GOBUILD=$(GOCMD) build

PLUGIN=eoss3.so
CLI=eoss3-cli
VERSITY_URL=https://github.com/versity/versitygw.git
VERSITY_DIR=$(shell pwd)/../versitygw

all: build

build: $(PLUGIN)

.PHONY: $(PLUGIN)
$(PLUGIN):
	$(GOBUILD) -buildmode=plugin -o $(PLUGIN) plugin.go

.PHONY: $(CLI)
$(CLI):
	$(GOBUILD) -o $(CLI) cli/main.go

install: $(PLUGIN) $(CLI)
	@cp $(PLUGIN) /usr/local/lib/eoss3.so
	@cp $(CLI) /usr/local/bin/eoss3-cli

.PHONY: clean
clean:
	@rm -f $(PLUGIN)
	@rm -f $(CLI)
