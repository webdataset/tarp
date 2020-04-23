module github.com/tmbdev/tarp/tarp

replace github.com/tmbdev/tarp/dpipes => ../dpipes

replace github.com/tmbdev/tarp/dpipes/messaging => ../dpipes/messaging

replace github.com/tmbdev/tarp/dpipes/sql => ../dpipes/sql

go 1.14

require (
	github.com/jessevdk/go-flags v1.4.0
	github.com/tmbdev/tarp/dpipes v0.0.0-00010101000000-000000000000
	github.com/tmbdev/tarp/dpipes/messaging v0.0.0-00010101000000-000000000000
	github.com/tmbdev/tarp/dpipes/sql v0.0.0-00010101000000-000000000000
)
