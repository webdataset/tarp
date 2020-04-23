package sql

import (
	"database/sql"
	"log"
	"os"
	"regexp"
	"strings"

	sq "github.com/Masterminds/squirrel"
	. "github.com/tmbdev/tarp/dpipes"
	// load the sqlite3 driver
	_ "github.com/mattn/go-sqlite3"
)

var errlog *log.Logger = log.New(os.Stderr, "", 0)

// SplitFields takes an array of field specs ("col:extension:dbtype")
// and returns three arrays containing col names, extension names, and
// dbtype names.
func SplitFields(fields []string) (columns, sources, types []string) {
	cleanup := regexp.MustCompile("[^a-zA-Z0-9]")
	columns = make([]string, len(fields))
	sources = make([]string, len(fields))
	types = make([]string, len(fields))
	for i, f := range fields {
		F := strings.SplitN(f, ":", 3)
		columns[i] = cleanup.ReplaceAllString(F[0], "_")
		if len(F) > 1 {
			sources[i] = F[1]
		} else {
			sources[i] = F[0]
		}
		if len(F) > 2 {
			types[i] = F[2]
		} else {
			types[i] = "BLOB"
		}
	}
	return
}

// GetFirstName finds the first name in a list of names separated by sep
func GetFirstName(key string, sep string) string {
	F := strings.Split(key, sep)
	return F[0]
}

// GetAll builds a new sample by retrieving all the keys using GetFirst
func GetAll(sample Sample, keys []string) []Bytes {
	result := make([]Bytes, len(keys))
	for i, key := range keys {
		result[i], _ = GetFirst(sample, key)
	}
	return result
}

// RepeatString repeats s n times.
func RepeatString(s string, n int) []string {
	result := make([]string, n)
	for i := 0; i < n; i++ {
		result[i] = s
	}
	return result
}

// CreateTableStmt constructs a statement the makes an SQL table for the given fields.
func CreateTableStmt(tname string, fields []string) string {
	columns, _, types := SplitFields(fields)
	s := "CREATE TABLE " + tname + "("
	for i, k := range columns {
		s += k
		s += " "
		t := types[i]
		if t == "" {
			s += "BLOB"
		} else {
			s += t
		}
		if i < len(columns)-1 {
			s += ", "
		}
	}
	s += ")"
	return s
}

// DBSinkCreateTable creates a table suitable for writing samples.
func DBSinkCreateTable(db *sql.DB, tname string, fields []string) {
	_, err := db.Exec("DROP TABLE IF EXISTS " + tname)
	Handle(err)
	stmt := CreateTableStmt(tname, fields)
	// Debug.Println(stmt)
	_, err = db.Exec(stmt)
	Handle(err)
}

// DBSink creates a table and writes samples to a db.
func DBSink(db *sql.DB, tname string, fields []string) func(inch Pipe) {
	DBSinkCreateTable(db, tname, fields)
	return DBSinkNoCreate(db, tname, fields)
}

// DBSinkNoCreate writes samples to a db; the table must already exists.
func DBSinkNoCreate(db *sql.DB, tname string, fields []string) func(inch Pipe) {
	return func(inch Pipe) {
		columns, sources, _ := SplitFields(fields)
		// FIXME defer and flush every 1-2 sec
		for sample := range inch {
			values := make([]interface{}, len(sources))
			for i, k := range sources {
				value, err := GetFirst(sample, k)
				Handle(err)
				values[i] = value
			}
			q := sq.Insert(tname).Columns(columns...).Values(values...)
			s, _, _ := q.ToSql()
			// Debug.Println(s)
			_, err := q.RunWith(db).Exec()
			Handle(err, s)
		}
	}
}

// DBSource reads samples from a database table.
func DBSource(db *sql.DB, tname string, fields []string, order string) func(outch Pipe) {
	return func(outch Pipe) {
		// Debug.Println("DBSource start")
		columns, sources, _ := SplitFields(fields)
		q := sq.Select(columns...).From(tname)
		if order != "" {
			q = q.OrderBy(order)
		}
		stmt, _, _ := q.ToSql()
		// Debug.Println(stmt)
		rows, err := db.Query(stmt)
		Handle(err)
		values := make([]Bytes, len(sources))
		pointers := make([]interface{}, len(sources))
		for i := range values {
			pointers[i] = &values[i]
		}
		for rows.Next() {
			rows.Scan(pointers...)
			sample := Sample{}
			for i, key := range sources {
				// Debug.Println(i, key, values[i], reflect.TypeOf(values[i]))
				sample[GetFirstName(key, ";")] = values[i]
			}
			// Debug.Println("DBSource", string(sample["__key__"]))
			outch <- sample
		}
		close(outch)
		// Debug.Println("DBSource done")
	}
}
