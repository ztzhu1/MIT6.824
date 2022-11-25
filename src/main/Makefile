FILE ?= pg-metamorphosis.txt
PLUGIN ?= wc

PHONY: build co worker clean

build:
	go build -race -buildmode=plugin ../mrapps/$(PLUGIN).go

co:
	go run -race mrcoordinator.go $(FILE)

worker: build
	go run -race mrworker.go $(PLUGIN).so

clean:
	rm mr-out-*
