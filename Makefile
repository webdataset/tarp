all: clean tarp

clean:
	go clean
	rm -f tarp

cmds := $(wildcard cmd/*.go)
datapipes := $(wildcard datapipes/*.go)

tarp: $(cmds) $(datapipes)
	go build -o tarp $(cmds)
	tarp -h

test:
	cd datapipes && go test -v

dtest:
	cd datapipes && debug=stdout go test -v | tee ../test.log

coverage:
	cd datapipes && go test -coverprofile=c.out
	cd datapipes && go tool cover -html=c.out -o coverage.html
	firefox datapipes/coverage.html
