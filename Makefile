GOCMD=go
GOBUILD=$(GOCMD) build

PLUGIN=eoss3.so

all: build

build: $(PLUGIN)

.PHONY: $(PLUGIN)
$(PLUGIN):
	$(GOBUILD) -buildmode=plugin -gcflags="all=-N -l" -o $(PLUGIN) plugin.go
	$(GOBUILD) -o eoss3-cli cli/main.go

.PHONY: clean
clean:
	@rm -f $(PLUGIN)
