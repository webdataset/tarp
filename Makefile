cmds := $(wildcard tarp/*.go)
dpipes := $(wildcard dpipes/*.go)

bin/tarp: $(cmds) $(dpipes)
	go clean
	go build -o bin/tarp $(cmds)
	bin/tarp -h

bin/tarp-full: $(cmds) $(dpipes)
	go clean
	go build -tags mpio -o bin/tarp $(cmds)
	bin/tarp -h

install: bin/tarp
	cp bin/tarp /usr/local/bin

test:
	cd dpipes && go test -v

dtest:
	cd dpipes && debug=stdout go test -v | tee ../test.log

coverage:
	cd dpipes && go test -coverprofile=c.out
	cd dpipes && go tool cover -html=c.out -o coverage.html
	firefox dpipes/coverage.html
