cmds := $(wildcard *.go)
dpipes := $(wildcard ../dpipes/*.go)
GOFLAGS ?= "-X main.version=`date -Iseconds`" -o tarp

tarp: $(cmds) $(dpipes)
	go clean
	go mod tidy
	go get -u
	go build $(GOFLAGS) $(cmds)
	./tarp -h


install: tarp
	sudo cp $(name) /usr/local/bin
