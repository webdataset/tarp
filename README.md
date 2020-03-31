[code metrics](https://goreportcard.com/report/github.com/tmbdev/tarp)

# The `tarp` Utility

Tarfiles are commonly used for storing large amounts of data in an efficient,
sequential access, compressed file format, in particualr for deep learning
applications. For processing and data transformation,
people usually unpack them, operate over the files, and tar up the result again.

The `tarp` utility is a port of the Python [tarproc](http://github.com/tmbdev/tarproc)
utilities to Go. The `tarp` utility is a single executable, a "Swiss army knife"
for dataset transformations.

Available commands are:

- create: create tar files from a list of tar paths and corresponding data sources
- cat: concatenate tar files
- proc: process tar files
- sort: sort tar files
- split: split tar files

For `tarp cat`, sources and destinations can be ZMQ URLs (specified using zpush/zpull,
zpub/zsub, or zr versions that reverse connect/bind). This permits very large
sorting, processing, and shuffling networks to be set up (Kubernetes is a good platform
for this).

Commands consistently take/require a "-o" for the output in order to avoid accidental
file clobbering. You can specify "-" if you want to output to stdout.

# Examples

Download a dataset from Google Cloud, shuffle it, and split it into shards containing
1000 training samples each:

```Bash
gsutil cat gs://bucket/file.tar | tarp sort - -o - | tarp split -c 1000 -o 'output-%06d.tar'
```

Create a dataset for images stored in directories whose names represent class labels,
creates shards consisting of 1000 images each, and upload them to Google cloud:

```Bash
for classdir in *; do
    test -d $classdir || continue
    for image in $classdir/*.png; do
        imageid=$(basename $image .png)
        echo "$imageid.txt text:$classdir"
        echo "$imageid.png file:$image"
    done
done |
sort |
tarp create -o - - |
tarp split -c 1000 -o 'dataset-%06d.tar' \
    -p 'gsutil cp %s gs://mybucket/; rm %s'
```

(Note that in an actual application, you probably want to shuffle the
samples in the text file you create after the sort command.)


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

This is fairly new software. The command line interface is fairly stable,
but the internal APIs may still change substantially.

Future work:

- high priority
    - add Github testing and release workflows
    - make function/library naming more consistent
    - add ParallelMapSamples
    - more documentation
    - add 'tarp sendeof'
    - add tensorcom tensor outputs
    - refactor GOpen/GCreate
    - create dispatch for GOpen/GCreate
- medium priority
    - switch sort backend from sqlite3 to bbolt or badger
    - performance optimizations (remove needless copying)
    - add different FnameSplit options
    - close to 100% test coverage for Go
    - more command line tests
    - Kubernetes examples for large scale processing
    - add basic image processing, decompression, etc. functionality
    - add Lua scripting to `tarp proc` for fast internal processing
    - switch to interface and registry for GOpen (from current ad hoc code)
    - spec: JSON files for inputs
- low priority
    - use Go libraries for accessing cloud/object storage directly
    - TFRecord/tf.Example interoperability
    - add JSON input to "tarp create"
    - add separator option to "tarp create"
