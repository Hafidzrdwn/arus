.PHONY: all build test race lint fuzz fmt tidy vuln

all: tidy fmt lint test build

build:
	go build ./...

test:
	go test -v ./...

race:
	go test -race -count=1 ./...

lint:
	golangci-lint run

fuzz:
	go test -list='^Fuzz' ./...

fmt:
	go fmt ./...

tidy:
	go mod tidy

vuln:
	govulncheck ./...
