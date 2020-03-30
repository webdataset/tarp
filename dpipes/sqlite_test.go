package dpipes

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func TestDBSink(t *testing.T) {
	fields := []string{"__key__", "a", "b"}
	db, err := sql.Open("sqlite3", "_test.db")
	Handle(err)
	inch := make(Pipe, 100)
	for i := 0; i < 20; i++ {
		inch <- Sample{
			"__key__": Bytes(fmt.Sprintf("%03d", i)),
			"a":       Bytes("A"),
			"b":       Bytes("B"),
		}
	}
	done := WaitFor(func() {
		DBSink(db, "test", fields)(inch)
	})
	close(inch)
	<-done
	{
		outch := make(Pipe, 100)
		DBSource(db, "test", fields, "")(outch)
		for i := 0; i < 20; i++ {
			sample := <-outch
			key := fmt.Sprintf("%03d", i)
			assert.Equal(t, "A", string(sample["a"]), "source")
			assert.Equal(t, "B", string(sample["b"]), "source")
			assert.Equal(t, key, string(sample["__key__"]), "key")
		}
	}
	{
		outch := make(Pipe, 100)
		DBSource(db, "test", fields, "__key__ DESC")(outch)
		for i := 0; i < 20; i++ {
			sample := <-outch
			key := fmt.Sprintf("%03d", 19-i)
			assert.Equal(t, "A", string(sample["a"]), "source")
			assert.Equal(t, "B", string(sample["b"]), "source")
			assert.Equal(t, key, string(sample["__key__"]), "key")
		}
	}
	{
		outch := make(Pipe, 100)
		DBSource(db, "test", fields, "RANDOM()")(outch)
		counts := map[string]int{}
		for i := 0; i < 20; i++ {
			counts[fmt.Sprintf("%03d", i)] = 0
		}
		for i := 0; i < 20; i++ {
			sample := <-outch
			assert.Equal(t, "A", string(sample["a"]), "source")
			assert.Equal(t, "B", string(sample["b"]), "source")
			counts[string(sample["__key__"])]++
		}
		for _, v := range counts {
			assert.Equal(t, v, 1, "bad count")
		}
	}
}
