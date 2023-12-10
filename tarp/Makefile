cmds := $(wildcard *.go)
dpipes := $(wildcard ../dpipes/*.go)
GO_BUILD_FLAGS ?= -ldflags "-X main.version=`date -Iseconds`"
TARP_BIN_NAME ?= tarp

tarp: $(cmds) $(dpipes)
	go clean
	go mod tidy
	go get -u
	go build $(GO_BUILD_FLAGS) -o $(TARP_BIN_NAME) $(cmds)
	$(TARP_BIN_NAME) -h


install: tarp
	sudo cp $(TARP_BIN_NAME) /usr/local/bin
