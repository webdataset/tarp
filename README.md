# The `tarp` Utility

Tarfiles are commonly used for storing large amounts of data in an efficient,
sequential access, compressed file format, in particualr for deep learning
applications. For processing and data transformation,
people usually unpack them, operate over the files, and tar up the result again.

The `tarp` utility is a port of the Python [tarproc](http://github.com/tmbdev/tarproc)
utilities to Go. The `tarp` utility is a single executable, a "Swiss army knife"
for dataset transformations.

```Bash
    $ gsutil cat gs://bucket/file.tar | tarp sort - -o - | tarp split -c 1000 -o 'output-%06d.tar'
```

Available commands are:

- cat: concatenate tar files
- proc: process tar files
- sort: sort tar files
- split: split tar files

For `tarp cat`, sources and destinations can be ZMQ URLs (specified using zpush/zpull,
zpub/zsub, or zr versions that reverse connect/bind). This permits very large
sorting, processing, and shuffling networks to be set up (Kubernetes is a good platform
for this).

# Internals

Internally, data processing is handled using goroutines and channels passing
around samples. Samples are simple key/value stores of type `map[string][]byte`.
Most processing steps are pipeline elements. The general programming style is:

```Go
func ProcessSamples(parameters...) func(inch Pipe, outch Pipe) {
	return func(inch Pipe, outch Pipe) {
		...
		for sample := range inch {
			...
		}
		...
		close(outch)
	}
}
```

Note that unlike simple Golang pipeline examples, the caller
allocates the output channel; this gives code building pipelines
out of processing stages a bit more control.
Furthermore, construction of pipeline elements
involves an outer and an inner function ("currying"). This lets us
write pipelines more naturally.
For example, you can write code like this:

```Go
source := TarSource(fname)
sink := TarSink(fname)
pipeline := Pipeline(
	SliceSamples(0, 100),
	LogProgress(10, "progress"),
	RenameSamples(renamings, false)
)
Processing(source, pipeline, sink)
```

The main processing library is in the `datapipes` subdirectory;
tests for the library functions are also found here (run with
`go test` in that subdirectory).
The toplevel command and its subcommands are defined in `cmd`.
Tests for the command line functions can be executed with `./run-tests`
from the top of the source tree.

# Status

This is alpha software, with little documentation and possible API changes.

Future work:

- make function/library naming more consistent
- add different FnameSplit options
- add ParallelMapSamples
- more documentation
- Kubernetes examples for large scale processing
- add basic image processing, decompression, etc. functionality
- add tensorcom tensor outputs
- use Go libraries for accessing cloud/object storage directly
- add Lua scripting to `tarp proc` for fast internal processing
