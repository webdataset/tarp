all: clean bin/tarp

clean:
	go clean

cmds := $(wildcard tarp/*.go)
datapipes := $(wildcard dpipes/*.go)

bin/tarp: $(cmds) $(datapipes)
	go build -o bin/tarp $(cmds)
	bin/tarp -h

test:
	cd dpipes && go test -v ./...

dtest:
	cd dpipes && debug=stdout go test -v ./... | tee ../test.log

coverage:
	cd dpipes && go test -v ./... -coverprofile=c.out
	cd dpipes && go tool cover -html=c.out -o coverage.html
	firefox dpipes/coverage.html
