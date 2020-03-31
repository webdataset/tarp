module github.com/tmbdev/tarp/tarp

replace github.com/tmbdev/tarp/dpipes => ../dpipes

go 1.14

require (
	github.com/jessevdk/go-flags v1.4.0
	github.com/tmbdev/tarp/dpipes v0.0.0-20200330014249-228e36f0b803
)
