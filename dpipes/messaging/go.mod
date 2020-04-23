module github.com/tmbdev/tarp/dpipes/messaging

replace github.com/tmbdev/tarp/dpipes => ../

go 1.14

require (
	github.com/shamaton/msgpack v1.1.1
	github.com/stretchr/testify v1.5.1
	github.com/tmbdev/tarp/dpipes v0.0.0-00010101000000-000000000000
	gopkg.in/zeromq/goczmq.v4 v4.1.0
)
