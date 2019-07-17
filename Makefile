#--- Makefile ----

GOCMD=go
GODEP=dep
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOFORMAT=$(GOCMD) fmt
BINARY_DIR=cmd
BINARY_NAME=title
CONFIG_DIR=./config
CONFIG_NAME=config.yaml

VERSION=`git describe --abbrev=0 --tags`
BUILD=`date +%FT%T%z`
LDFLAGS=-ldflags "-X main.Version=${VERSION} -X main.Build=${BUILD}"

build-linux64:
		CGO_ENABLED=1 GOOS=linux GOARCH=amd64 $(GOBUILD) ${LDFLAGS} -o ./$(BINARY_DIR)/$(BINARY_NAME)_linux64 -v
build-linux32:
		CGO_ENABLED=1 GOOS=linux GOARCH=386 $(GOBUILD) ${LDFLAGS} -o ./$(BINARY_DIR)/$(BINARY_NAME)_linux32 -v
build-windows32:
        CGO_ENABLED=1 GOOS=windows GOARCH=386 CC=i686-w64-mingw32-gcc CXX=i686-w64-mingw32-g++ $(GOBUILD) ${LDFLAGS} -o ./$(BINARY_DIR)/$(BINARY_NAME)32.exe -v
build-windows64:
        CGO_ENABLED=1 GOOS=windows GOARCH=amd64 CC=x86_64-w64-mingw32-gcc CXX=x86_64-w64-mingw32-g++ $(GOBUILD) ${LDFLAGS} -o ./$(BINARY_DIR)/$(BINARY_NAME)64.exe -v
docker-build:
		docker run --rm -it -v "$(GOPATH)":/go -w <registry> golang:latest go build -o "./$(BINARY_DIR)/$(BINARY_NAME)" -v

all: test build
build: build-linux64
test:
		$(GOTEST) -v -cover --race ./...
clean:
		$(GOCLEAN)
		rm -rf ./$(BINARY_DIR)/$(BINARY_NAME)*
run:
		$(GOBUILD) -o ./$(BINARY_DIR)/$(BINARY_NAME) -v
		cp ${CONFIG_DIR}/${CONFIG_NAME} ./$(BINARY_DIR)/
		./$(BINARY_DIR)/$(BINARY_NAME) -cfg ./$(BINARY_DIR)/${CONFIG_NAME}
deps:
		$(GODEP) ensure -v
init:
		$(GODEP) init -v
