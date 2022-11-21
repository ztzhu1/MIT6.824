FILE ?= no_file

PHONY: build co worker

build:
	go build -race -buildmode=plugin ../mrapps/wc.go

co:
	go run -race mrcoordinator.go $(FILE)

worker: build
	go run -race mrworker.go wc.so
