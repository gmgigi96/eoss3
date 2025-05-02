GOCMD=go
GOBUILD=$(GOCMD) build

PLUGIN=eoss3.so
VERSITY_URL=https://github.com/versity/versitygw.git
VERSITY_DIR=$(shell pwd)/../versitygw

all: build

build: $(PLUGIN)

.PHONY: $(PLUGIN)
$(PLUGIN):
	$(GOBUILD) -buildmode=plugin -o $(PLUGIN) plugin.go
	$(GOBUILD) -o eoss3-cli cli/main.go

prepare:
	@test -d "$(VERSITY_DIR)" || (echo "$(VERSITY_DIR) does not exist, cloning..."; git clone "$(VERSITY_URL)" "$(VERSITY_DIR)")
	@pushd $(VERSITY_DIR); make; cp versitygw /usr/local/bin/versitygw; popd
	@make build
	@cp eoss3-cli /usr/local/bin/eoss3-cli
	@cp eoss3.so /usr/local/lib/eoss3.so

.PHONY: clean
clean:
	@rm -f $(PLUGIN)
