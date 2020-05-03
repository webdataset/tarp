package main

import (
	"io/ioutil"
	"math/rand"
	"os"
	"strings"

	"github.com/dgraph-io/badger/v2"
	"github.com/shamaton/msgpack"
	"github.com/tmbdev/tarp/dpipes"
)

var sortopts struct {
	Fields     string `short:"f" long:"field" description:"fields to extract; name or name:old1;old2;old3"`
	Output     string `short:"o" long:"outputs" description:"output file"`
	Sortfields string `short:"s" long:"sortfield" description:"fields to sort on" default:"__random__"`
	Slice      string `long:"slice" description:"input slice"`
	Tmpdir     string `long:"tmpdir" description:"temporary storage for sorting" default:""`
	Keepdb     bool   `long:"keepdb"`
	Rekey      string `long:"rekey"`
	Positional struct {
		Inputs []string `required:"yes"`
	} `positional-args:"yes"`
}

func getdbdir() string {
	dbdir := ""
	if sortopts.Tmpdir != "" {
		dbdir = sortopts.Tmpdir
	} else {
		s, err := ioutil.TempDir("", "dbdir")
		Handle(err)
		dbdir = s
	}
	return dbdir

}

func WriteBadger(db badger.DB, sortfields []string) func(dpipes.Pipe) {
	return func(inch dpipes.Pipe) {
		for sample := range inch {
			dpipes.Assert(len(sample["__key__"]) > 0, "encode")
			data, err := msgpack.Encode(sample)
			Handle(err)
			key := make(dpipes.Bytes, 8)
			if sortfields[0] == "__random__" {
				rand.Read(key)
			} else {
				key = sample[sortfields[0]]
			}
			dpipes.Assert(len(key) > 0)
			err = db.Update(func(txn *badger.Txn) error {
				dpipes.Debug.Println("db write", string(sample["__key__"]))
				err = txn.Set(key, data)
				return err
			})
			Handle(err)
		}
		dpipes.Debug.Println("db done writing")
	}
}

func ReadBadger(db badger.DB) func(dpipes.Pipe) {
	return func(outch dpipes.Pipe) {
		defer func() {
			dpipes.Debug.Println("db close outch")
			close(outch)
		}()
		err := db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = 10
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				err := item.Value(func(value []byte) error {
					// msgpack.Decode points into byte array, but array
					// is reused by it.Next(); that's why we need to copy
					value1 := make([]byte, len(value))
					copy(value1, value)
					sample := dpipes.Sample{}
					msgpack.Decode(value1, &sample)
					dpipes.Assert(len(sample["__key__"]) > 0, "decode")
					dpipes.Debug.Println("db read", string(sample["__key__"]))
					outch <- sample
					return nil
				})
				Handle(err)
			}
			dpipes.Debug.Println("db done reading")
			return nil
		})
		Handle(err)
	}
}

func sortcmd() {
	Validate(len(sortopts.Positional.Inputs) > 0, "must provide at least one input (can be '-')")
	Validate(sortopts.Fields != "", "you must provide some fields")
	fields := []string{"__key__"}
	fields = append(fields, strings.Split(sortopts.Fields, " ")...)
	Validate(sortopts.Output != "", "must provide output (can be '-')")
	sortfields := strings.Split(sortopts.Sortfields, " ")
	Validate(len(sortfields) == 1, "only one sort field can be specified")
	verbose.Println("# inputs", sortopts.Positional.Inputs)
	verbose.Println("# start:end", sortopts.Slice)

	dbdir := getdbdir()
	defer os.RemoveAll(dbdir)
	dboptions := badger.DefaultOptions(dbdir)
	dboptions.Logger = nil
	db, err := badger.Open(dboptions)
	Handle(err)
	defer func() {
		dpipes.Debug.Println("db close")
		db.Close()
	}()

	{
		verbose.Println("writing")
		processes := make([]dpipes.Process, 0, 100)
		processes = append(processes, dpipes.SliceSamplesSpec(sortopts.Slice))
		if sortopts.Fields != "" {
			processes = append(processes, dpipes.RenameSamples(fields, true))
		}
		dpipes.Processing(
			dpipes.TarSources(sortopts.Positional.Inputs, nil),
			dpipes.Pipeline(processes...),
			WriteBadger(*db, sortfields),
		)
	}

	{
		verbose.Println("reading")
		processes := make([]dpipes.Process, 0, 100)
		if sortopts.Rekey != "" {
			processes = append(processes, dpipes.RekeySamples(sortopts.Rekey))
		} else {
			processes = append(processes, dpipes.CopySamples)
		}
		dpipes.Processing(
			ReadBadger(*db),
			dpipes.Pipeline(processes...),
			dpipes.TarSinkFile(sortopts.Output),
		)
	}
}

func init() {
	Parser.AddCommand("sort", "sort tar files", "", &sortopts)
	Commands["sort"] = sortcmd
}
