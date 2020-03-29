module github.com/tmbdev/tarp

replace github.com/tmbdev/tarp/datapipes => ./datapipes

go 1.14

require (
	github.com/jessevdk/go-flags v1.4.0
	github.com/spf13/pflag v1.0.5
	github.com/tmbdev/tarp/datapipes v0.0.0-00010101000000-000000000000
)
