// Copyright 2021 Hans-Werner Heinzen. All rights reserved.
// Use of this source code is governed by a license
// that can be found in the LICENSE file.

// SQL syntax differs slightly for different databases.
// Here are the specifics for SQLite.
// sqlite shall be defaukt.
//
// NOT mysql AND NOT postgresql
// +build !mysql,!postgresql

package livedb

import (
	"database/sql"
	"fmt"

	_ "rsc.io/sqlite"

	. "github.com/hwheinzen/stringl10n/mistake"
)

// gDbDriver is the name of the database driver.
const gDbDriver = "sqlite3"

const Now = "now" // keyword Now can be used instead of a timestamp string

const gTmspFormat = "2006-01-02 15:04:05.000"

var StdAtts = []string{
	"id",
	"begin",
	"until",
	"pkey",
	"created",
	"createdby",
	"ended", // 'terminated' is reserved word for MariaDB/MySQL
	"endedby",
}

var StdDefs = []string{
	"id integer not null",
	"begin varchar(26) not null",
	"until varchar(26)",
	"pkey integer primary key autoincrement",
	"created varchar(26) not null",
	"createdby varchar(50) not null",
	"ended varchar(26)", // 'terminated' is reserved word for MariaDB/MySQL
	"endedby varchar(50)",
}

var stdIDAtts = []string{
	"id",
	"created",
	"createdby",
	"usedby",
}

var stdIDDefs = []string{
	"id integer primary key autoincrement",
	"created varchar(26) not null",
	"createdby varchar(50) not null",
	"usedby varchar(50)",
}

// FormatTmsp returns a string containing the SQL formatting
// of a timestamp attribute in Sqlite syntax.
func FormatTmsp(num int) string {
	return "strftime('%Y-%m-%d %H:%M:%f',?)"
}

// FormatNow returns a string containing the SQL formatting
// of the current timestamp in Sqlite syntax.
func FormatNow() string {
	return "strftime('%Y-%m-%d %H:%M:%f','now')"
}

// FormatDiffTmsp returns a string containing the SQL formatting
// of the difference of two timestamp attributes in Sqlite syntax.
func FormatDiffTmsp(ref, tmsp *string) string {
	// ref, tmsp: not needed here
	return "strftime('%s',?) - strftime('%s',?)"
}

// FormatDiffNow returns a string containing the SQL formatting
// of the difference between a given timestamp attribute
// and the current timestamp in Sqlite syntax.
func FormatDiffNow() string {
	return "strftime('%s','now') - strftime('%s',?)"
}

// FormatAtt returns a string containing the SQL formatting
// of a simple attribute in Sqlite syntax.
func FormatAtt(num int) string {
	return "?"
}

// FormatNull returns a string containing NULL.
func FormatNull() string {
	return "NULL"
}

func (t *Table) insertedKey(tx *sql.Tx, r sql.Result) (int, error) {
	fnc := "Table.insertedKey"

	n, err := r.LastInsertId() // from autoincrement pkey
	if err != nil {
		e := Err{Fix: "LIVEDB:get last inserted key failed"}
		return 0, fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}
	return int(n), nil
}

func (t *Table) insertedID(tx *sql.Tx, r sql.Result) (int, error) {
	fnc := "Table.insertedKey"

	n, err := r.LastInsertId() // from autoincrement pkey
	if err != nil {
		e := Err{Fix: "LIVEDB:get last inserted id failed"}
		return 0, fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}
	return int(n), nil
}
