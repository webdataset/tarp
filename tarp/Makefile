cmds := $(wildcard *.go)
dpipes := $(wildcard ../dpipes/*.go)

tarp: $(cmds) $(dpipes)
	go clean
	go get -u
	go build -ldflags "-X main.version=`date -Iseconds`" -o tarp $(cmds)
	tarp -h

tarp-full: $(cmds) $(dpipes)
	go clean
	go get -u
	go build -tags mpio -ldflags "-X main.version=mpio-`date -Iseconds`" -o tarp $(cmds)
	bin/tarp -h

install: tarp
	sudo cp tarp /usr/local/bin
