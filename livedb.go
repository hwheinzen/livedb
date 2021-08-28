// Copyright 2021 Hans-Werner Heinzen. All rights reserved.
// Use of this source code is governed by a license
// that can be found in the LICENSE file.

// Package livedb provides standard functions for the handling of livedb tables.
//
// Livedb tables have the following properties:
//  - Each row in a table is valid for a period (begin-timestamp/until-timestamp).
//  - Changing data in a row means terminating this period
//    and creating a new row with the changed data and a new period starting.
//  - Valid data will be neither overwritten nor deleted;
//    changing the past is not allowed;
//    overwrite and delete are possible for not yet valid (future period) data.
package livedb

import (
	"bytes"
	"database/sql"
	"fmt"

	. "github.com/hwheinzen/stringl10n/mistake"
)

const pkg = "livedb"

// GDb is the global livedb database object.
// Use it for non-standard read access (selects with joined tables, etc).
var GDb *sql.DB

// Open opens the livedb database and assigns it to the global database object.
func Open(openString string) error {
	fnc := "Open"

	if GDb != nil {
		err := Err{Fix: "LIVEDB:database already open"}
		return fmt.Errorf(fnc+":%w", err)
	}

	db, err := sql.Open(gDbDriver, openString)
	if err != nil {
		e := Err{Fix: "LIVEDB:open database failed"}
		return fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}
	GDb = db

	Log("open database")

	return nil
}

// Close closes the livedb database and deletes the global database object.
func Close() error {
	fnc := "Close"

	if GDb == nil { // already closed -> ok
		return nil
	}

	err := GDb.Close()
	if err != nil {
		e := Err{Fix: "LIVEDB:close database failed"}
		return fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}
	GDb = nil

	Log("close database")

	return nil
}

// Begin starts a transaction and returns a transaction object.
func Begin() (tx *sql.Tx, err error) {
	fnc := "Begin"

	tx, err = GDb.Begin()
	if err != nil {
		e := Err{Fix: "LIVEDB:begin transaction failed"}
		return nil, fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	Log("begin transaction")

	return tx, nil
}

// Commit ends a transaction and deletes the transaction object.
func Commit(tx *sql.Tx) error {
	fnc := "Commit"

	if tx == nil {
		err := Err{Fix: "LIVEDB:no transaction to commit"}
		return fmt.Errorf(fnc+":%w", err)
	}

	err := tx.Commit()
	if err != nil {
		e := Err{Fix: "LIVEDB:commit transaction failed"}
		return fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	Log("commit transaction")

	return nil
}

// Rollback rolls back a transaction and deletes the  transaction object.
func Rollback(tx *sql.Tx) error {
	fnc := "Rollback"

	if tx == nil {
		err := Err{Fix: "LIVEDB:no transaction to rollback"}
		return fmt.Errorf(fnc+":%w", err)
	}

	err := tx.Rollback()
	if err != nil {
		e := Err{Fix: "LIVEDB:rollback transaction failed"}
		return fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	Log("rollback transaction")

	return nil
}

// Std consists of all the livedb standard attributes.
type Std struct {
	ID        int    // specifically typed ID may be used in some API
	Begin     string // valid from and including timestamp
	Until     string // valid until and excluding timestamp
	Pkey      int    // primary key
	Created   string // created timestamp
	CreatedBy string // created by
	Ended     string // terminated timestamp
	EndedBy   string // terminated by
}

// NOTE: Not every database supports date/time/timestamp data types (e.g. Sqlite).
// We store such data as text/varchar. (See: livedb_sqlite.go)

type stdID struct {
	id        int
	created   string // created timestamp
	createdBy string // created by
	usedBy    string // creator of row in original table
}

// Record combines livedb standard and individual attributes.
type Record struct {
	Std             // standard attributes
	Idv interface{} // individual attributes
}

// ScanFunc is a function type.
// It takes the result of a SQL query and returns a record struct, Std part,
// and the specific part of it corresponding to the queried database table.
type ScanFunc func(*sql.Rows) (Record, error)

// ValsFunc is a function type.
// It takes a struct that corresponds to an database object and returns
// an interface slice containing values in the order the struct fields are defined.
// An empty string indicates a NULL value.
type ValsFunc func(interface{}) []string

//type ValsFunc func(interface{}) []interface{} // problem: null

type Table struct {
	Name string   // table name
	Defs []string // attibute definitions
	Atts []string // attibute names
	Old  Record
	New  Record
	Vals ValsFunc
	Scan ScanFunc
}

// Table.Create creates a livedb table with the correspondend ID-table (initialized).
//
// NOTE: Table.Create works with transaction tx for Sqlite,
// but not for Postgres - use nil there.
func (t *Table) Create(tx *sql.Tx) error {
	fnc := "Table.Create"

	err := t.createPrecs() // preconditions
	if err != nil {
		return fmt.Errorf(fnc+":%w", err)
	}

	ok, err := t.exists(tx)
	if err != nil {
		return fmt.Errorf(fnc+":%w", err)
	}
	if ok {
		return nil // already created
	}

	err = t.create(tx)
	if err != nil {
		return fmt.Errorf(fnc+":%w", err)
	}

	return nil
}

func (t *Table) createPrecs() error {
	fnc := "Table.createPrecs"

	if GDb == nil {
		err := Err{Fix: "LIVEDB:create needs database object"}
		return fmt.Errorf(fnc+":%w", err)
	}

	if t.Name == "" {
		err := Err{Fix: "LIVEDB:table name missing"}
		return fmt.Errorf(fnc+":%w", err)
	}

	// allow "empty" tables
// 	if len(t.Defs) < 1 {
// 		err := Err{
// 			Fix: "LIVEDB:{{.Name}} missing for {{.Table}}",
// 			Var: []struct {
// 				Name  string
// 				Value interface{}
// 			}{
// 				{"Name", "Table.Defs"},
// 				{"Table", t.Name},
// 			},
// 		}
// 		return fmt.Errorf(fnc+":%w", err)
// 	}

	return nil
}

func (t *Table) create(tx *sql.Tx) error {
	fnc := "Table.create"

	err := t.createTable(tx)
	if err != nil {
		return fmt.Errorf(fnc+":%w", err)
	}

	err = t.createIDTable(tx)
	if err != nil {
		return fmt.Errorf(fnc+":%w", err)
	}

	err = t.createIndexIDBegin(tx)
	if err != nil {
		return fmt.Errorf(fnc+":%w", err)
	}

	err = t.createIndexIDUntil(tx)
	if err != nil {
		return fmt.Errorf(fnc+":%w", err)
	}

	return nil
}

func (t *Table) createTable(tx *sql.Tx) error {
	fnc := "Table.createTable"

	var buf bytes.Buffer
	var put = buf.WriteString
	var err error

	put("create table " + t.Name + "(") // <======== create table
	for _, def := range StdDefs {
		put(def + ",")
	}
	for _, def := range t.Defs {
		put(def + ",")
	}

	s := buf.String()
	s = s[:len(s)-1] + ");" // replace last comma

	Log("s:", s)

	if tx != nil {
		_, err = tx.Exec(s)
	} else {
		_, err = GDb.Exec(s)
	}
	if err != nil {
		e := Err{
			Fix: "LIVEDB:error creating table by:{{.Query}}",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Query", s},
			},
		}
		return fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	return nil
}

func (t *Table) createIDTable(tx *sql.Tx) error {
	fnc := "Table.createIDTable"

	var buf bytes.Buffer
	var put = buf.WriteString
	var err error

	put("create table " + t.Name + "id(") // <======== create id-table
	for _, def := range stdIDDefs {
		put(def + ",")
	}
	s := buf.String()
	s = s[:len(s)-1] + ");" // replace last comma

	Log("s:", s)

	if tx != nil {
		_, err = tx.Exec(s)
	} else {
		_, err = GDb.Exec(s)
	}
	if err != nil {
		e := Err{
			Fix: "LIVEDB:error creating table by:{{.Query}}",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Query", s},
			},
		}
		return fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	return nil
}

func (t *Table) createIndexIDBegin(tx *sql.Tx) error {
	fnc := "Table.createIndexIDBegin"

	var buf bytes.Buffer
	var put = buf.WriteString
	var err error

	put("create unique index " + t.Name + "idxidbegin on " + t.Name + " (id, begin);")

	s := buf.String()

	Log("s:", s)

	if tx != nil {
		_, err = tx.Exec(s)
	} else {
		_, err = GDb.Exec(s)
	}
	if err != nil {
		e := Err{
			Fix: "LIVEDB:error creating index by:{{.Query}}",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Query", s},
			},
		}
		return fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	return nil
}

func (t *Table) createIndexIDUntil(tx *sql.Tx) error {
	fnc := "Table.createIndexIDUntil"

	var buf bytes.Buffer
	var put = buf.WriteString
	var err error

	put("create index " + t.Name + "idxiduntil on " + t.Name + " (id, until);")

	s := buf.String()

	Log("s:", s)

	if tx != nil {
		_, err = tx.Exec(s)
	} else {
		_, err = GDb.Exec(s)
	}
	if err != nil {
		e := Err{
			Fix: "LIVEDB:error creating index by:{{.Query}}",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Query", s},
			},
		}
		return fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	return nil
}

func (t *Table) exists(tx *sql.Tx) (bool, error) {
	fnc := "Table.exists"

	var err1, err2 error

	s := "select count(*) from " + t.Name + ";"
	Log("s:", s)
	if tx != nil {
		_, err1 = tx.Query(s)
	} else {
		_, err1 = GDb.Query(s)
	}

	s = "select count(*) from " + t.Name + "id;"
	Log("s:", s)
	if tx != nil {
		_, err2 = tx.Query(s)
	} else {
		_, err2 = GDb.Query(s)
	}

	switch {
	case err1 == nil && err2 != nil:
		e := Err{
			Fix: "LIVEDB:table {{.Name}} exists, table {{.Nam2}} is missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", t.Name},
				{"Nam2", t.Name + "id"},
			},
		}
		return false, fmt.Errorf(fnc+":%w:"+err2.Error(), e)
	case err2 == nil && err1 != nil:
		e := Err{
			Fix: "LIVEDB:table {{.Name}} exists, table {{.Nam2}} is missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", t.Name + "id"},
				{"Nam2", t.Name},
			},
		}
		return false, fmt.Errorf(fnc+":%w:"+err1.Error(), e)
	case err2 == nil && err1 == nil:
		return true, nil
	default:
		return false, nil
	}
}
