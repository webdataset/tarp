all: clean bin/tarp

clean:
	go clean

cmds := $(wildcard tarp/*.go)
datapipes := $(wildcard dpipes/*.go)

bin/tarp: $(cmds) $(datapipes)
	go build -o bin/tarp $(cmds)
	bin/tarp -h

bin/tarp-full: $(cmds) $(datapipes)
	go build -tags mpio -o bin/tarp $(cmds)
	bin/tarp -h

test:
	cd datapipes && go test -v

dtest:
	cd datapipes && debug=stdout go test -v | tee ../test.log

coverage:
	cd datapipes && go test -coverprofile=c.out
	cd datapipes && go tool cover -html=c.out -o coverage.html
	firefox datapipes/coverage.html
