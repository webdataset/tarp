module github.com/tmbdev/tarp/dpipes/sql

replace github.com/tmbdev/tarp/dpipes => ../

go 1.14

require (
	github.com/Masterminds/squirrel v1.2.0
	github.com/mattn/go-sqlite3 v2.0.3+incompatible
	github.com/stretchr/testify v1.5.1
	github.com/tmbdev/tarp/dpipes v0.0.0-00010101000000-000000000000
)
