FILE ?= pg-metamorphosis.txt

PHONY: build co worker clean

build:
	go build -race -buildmode=plugin ../mrapps/wc.go

co:
	go run -race mrcoordinator.go $(FILE)

worker: build
	go run -race mrworker.go wc.so

clean:
	rm mr-out*
