GOCMD=go
GOBUILD=$(GOCMD) build

PLUGIN=eoss3.so

all: build

build: $(PLUGIN)

.PHONY: $(PLUGIN)
$(PLUGIN):
	$(GOBUILD) -buildmode=plugin -o $(PLUGIN) plugin.go

.PHONY: clean
clean:
	@rm -f $(PLUGIN)