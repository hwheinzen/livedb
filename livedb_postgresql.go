// Copyright 2021 Hans-Werner Heinzen. All rights reserved.
// Use of this source code is governed by a license
// that can be found in the LICENSE file.

// SQL syntax differs slightly for different databases.
// Here are the specifics for PostgreSQL.
//
// +build postgresql

package livedb

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq" // Postgres db

	. "github.com/hwheinzen/stringl10n/mistake"
)

const gDbDriver = "postgres"

const Now = "now" // keyword Now can be used instead of a timestamp string

const gTmspFormat = "2006-01-02 15:04:05.000000"

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
	"pkey serial",
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
	`"id" serial`,
	"created varchar(26) not null",
	"createdby varchar(50) not null",
	"usedby varchar(50)",
}

// FormatTmsp returns a string containing the SQL formatting
// of a timestamp attribute in PostgreSQL syntax.
func FormatTmsp(num int) string {
	return "to_char($" + fmt.Sprint(num) + "::timestamp,'YYYY-MM-DD HH24:MI:SS.US')"
}

// FormatNow returns a string containing the SQL formatting
// of the current timestamp in PostgreSQL syntax.
//
// Postgres' now() returns allways timestamp of begin transaction!
// Use clock_timestamp() instead.
func FormatNow() string {
	//return "to_char(now() at time zone 'utc','YYYY-MM-DD HH24:MI:SS.US')"
	return "to_char(clock_timestamp() at time zone 'utc','YYYY-MM-DD HH24:MI:SS.US')"
}

// FormatDiffTmsp returns a string containing the SQL formatting
// of the difference of two timestamp attributes in PostgreSQL syntax.
func FormatDiffTmsp(ref, tmsp *string) string {
	// ref, tmsp: not needed here
	return "extract( epoch from $1::timestamp - $2::timestamp)"
}

// FormatDiffNow returns a string containing the SQL formatting
// of the difference between a given timestamp attribute
// and the current timestamp in PostgreSQL syntax.
func FormatDiffNow() string {
	return "extract( epoch from now() at time zone 'utc' - $1::timestamp)"
}

// FormatAtt returns a string containing the SQL formatting
// of a simple attribute in PostgreSQL syntax.
func FormatAtt(num int) string {
	return "$" + fmt.Sprint(num)
}

// FormatNull returns a string containing NULL.
func FormatNull() string {
	return "NULL"
}

func (t *Table) insertedKey(tx *sql.Tx, r sql.Result) (key int, err error) {
	fnc := "Table.insertedKey"

	rows := &sql.Rows{}

	s := "select last_value from " + t.Name + "_pkey_seq;"

	Log("s:", s)

	if tx != nil {
		rows, err = tx.Query(s)
	} else {
		rows, err = GDb.Query(s)
	}
	if err != nil {
		e := Err{Fix: "LIVEDB:select last inserted key failed"}
		return 0, fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}
	defer rows.Close()

	if rows.Next() {
		err = rows.Scan(&key) // inserted key
		if err != nil {
			e := Err{Fix: "LIVEDB:scan last inserted key failed"}
			return 0, fmt.Errorf(fnc+":%w:"+err.Error(), e)
		}
	}
	err = rows.Err()
	if err != nil {
		e := Err{Fix: "LIVEDB:get last inserted key failed"}
		return 0, fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	return key, nil
}

func (t *Table) insertedID(tx *sql.Tx, r sql.Result) (n int, err error) {
	fnc := "Table.insertedID"

	rows := &sql.Rows{}

	s := "select last_value from " + t.Name + "ID_id_seq;"

	Log("s:", s)

	if tx != nil {
		rows, err = tx.Query(s)
	} else {
		rows, err = GDb.Query(s)
	}
	if err != nil {
		e := Err{Fix: "LIVEDB:select last inserted id failed"}
		return 0, fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}
	defer rows.Close()

	if rows.Next() {
		err = rows.Scan(&n) // inserted id
		if err != nil {
			e := Err{Fix: "LIVEDB:scan last inserted id failed"}
			return 0, fmt.Errorf(fnc+":%w:"+err.Error(), e)
		}
	}
	err = rows.Err()
	if err != nil {
		e := Err{Fix: "LIVEDB:get last inserted id failed"}
		return 0, fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	return n, nil
}
