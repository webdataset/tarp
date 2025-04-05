module github.com/tmbdev/tarp/tarp

replace github.com/tmbdev/tarp/dpipes => ../dpipes

go 1.23.0

toolchain go1.24.1

require (
	github.com/dgraph-io/badger/v3 v3.2103.5
	github.com/jessevdk/go-flags v1.6.1
	github.com/shamaton/msgpack v1.2.1
	github.com/tmbdev/tarp/dpipes v0.0.0-20221009163818-4aac5677b928
)

require (
	github.com/Masterminds/squirrel v1.5.4 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgraph-io/ristretto v0.2.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20241129210726-2c02b8208cf8 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/flatbuffers v25.2.10+incompatible // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/lann/builder v0.0.0-20180802200727-47ae307949d0 // indirect
	github.com/lann/ps v0.0.0-20150810152359-62de8c46ede0 // indirect
	github.com/mattn/go-sqlite3 v1.14.27 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	go.opencensus.io v0.24.0 // indirect
	golang.org/x/net v0.38.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	google.golang.org/protobuf v1.36.6 // indirect
	gopkg.in/zeromq/goczmq.v4 v4.1.0 // indirect
)
