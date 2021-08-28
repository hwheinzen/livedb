// Copyright 2021 Hans-Werner Heinzen. All rights reserved.
// Use of this source code is governed by a license
// that can be found in the LICENSE file.

// SQL syntax differs slightly for different databases.
// Here are the specifics for MySQL.
//
// +build mysql

package livedb

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql" // Mysql db

	. "github.com/hwheinzen/stringl10n/mistake"
)

const gDbDriver = "mysql"

const gTmspFormat = "2006-01-02 15:04:05.000000"

const Now = "now()" // keyword Now can be used instead of a timestamp string

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
	"pkey integer auto_increment primary key",
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
	"id integer auto_increment primary key",
	"created varchar(26) not null",
	"createdby varchar(50) not null",
	"usedby varchar(50)",
}

// FormatTmsp returns a string containing the SQL formatting
// of a timestamp attribute in Mysql syntax.
func FormatTmsp(num int) string {
	return "date_format(?,'%Y-%m-%d %H:%i:%s.%f')"
}

// FormatNow returns a string containing the SQL formatting
// of the current timestamp in Mysql syntax.
func FormatNow() string {
	return "date_format(utc_timestamp(6),'%Y-%m-%d %H:%i:%s.%f')"
}

// FormatDiffTmsp returns a string containing the SQL formatting
// of the difference of two timestamp attributes in Mysql syntax.
func FormatDiffTmsp(ref, tmsp *string) string {
	*ref, *tmsp = *tmsp, *ref          // switch arguments in caller
	return "timestampdiff(second,?,?)" // #### 2-1 !!! TODO
}

// FormatDiffNow returns a string containing the SQL formatting
// of the difference between a given timestamp attribute
// and the current timestamp in Mysql syntax.
func FormatDiffNow() string {
	return "timestampdiff(second,?,utc_timestamp(6))"
}

// FormatAtt returns a string containing the SQL formatting
// of a simple attribute in Mysql syntax.
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
