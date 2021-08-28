// Copyright 2021 Hans-Werner Heinzen. All rights reserved.
// Use of this source code is governed by a license
// that can be found in the LICENSE file.

package livedb

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"

	. "github.com/hwheinzen/stringl10n/mistake"
)

func (t *Table) readID(id int, tx *sql.Tx) (stdID, error) {
	fnc := "Table.readID"

	var buf bytes.Buffer
	var put = buf.WriteString // write method

	sqlargs := []interface{}{}
	num := int(0)

	put("select ")
	for i, att := range stdIDAtts {
		if i == 0 { // first one
			put(att)
			continue
		}
		put("," + att)
	}
	put(" from " + t.Name + "id")

	num++
	put(" where id=" + FormatAtt(num) + ";")
	sqlargs = append(sqlargs, fmt.Sprint(id))

	s := buf.String()

	Log("s:", s)
	Log("sqlargs:", sqlargs)

	stdID, err := t.queryID(s, sqlargs, tx)
	if err != nil {
		return stdID, fmt.Errorf(fnc+":%w:", err)
	}

	return stdID, nil
}

func (t *Table) queryID(s string, sqlargs []interface{}, tx *sql.Tx) (stdID, error) {
	fnc := "Table.queryID"

	var stdID stdID

	rows := &sql.Rows{}
	var err error

	if tx != nil {
		rows, err = tx.Query(s, sqlargs...)
	} else {
		rows, err = GDb.Query(s, sqlargs...)
	}
	if err != nil {
		e := Err{Fix: "LIVEDB:error executing query:{{.Query}}",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Query", s},
			},
		}
		return stdID, fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}
	defer rows.Close()

	if !rows.Next() {
		err = rows.Err()
		if err != nil {
			e := Err{Fix: "LIVEDB:error at rows.Next for query:{{.Query}}",
				Var: []struct {
					Name  string
					Value interface{}
				}{
					{"Query", s},
				},
			}
			return stdID, fmt.Errorf(fnc+":%w:"+err.Error(), e)
		}
		return stdID, nil // NOTFOUND
	}

	stdID, err = scanID(rows) // call the given scan function
	if err != nil {
		e := Err{Fix: "LIVEDB:error at scan(rows) for query:{{.Query}}",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Query", s},
			},
		}
		return stdID, fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	return stdID, nil
}

func scanID(rows *sql.Rows) (stdID, error) {
	fnc := "scanID"

	stdID := stdID{}
	nullStr1 := sql.NullString{}

	err := rows.Scan(
		&(stdID.id),
		&(stdID.created), &(stdID.createdBy),
		&(nullStr1), // UsedBy
	)
	if err != nil {
		e := Err{Fix: "LIVEDB:error scanning row"}
		return stdID, fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	if nullStr1.Valid {
		stdID.usedBy = nullStr1.String
	}

	return stdID, nil
}

func (t *Table) countByID(id int, tx *sql.Tx) (int, error) {
	fnc := "Table.countByID"

	var buf bytes.Buffer
	var put = buf.WriteString // write method

	sqlargs := []interface{}{}
	num := int(0)

	put("select count(*)")
	put(" from " + t.Name)

	num++
	put(" where id=" + FormatAtt(num))
	sqlargs = append(sqlargs, fmt.Sprint(id))

	s := buf.String()

	Log("s:", s)
	Log("sqlargs:", sqlargs)

	rows := &sql.Rows{}
	var err error
	if tx != nil {
		rows, err = tx.Query(s, sqlargs...)
	} else {
		rows, err = GDb.Query(s, sqlargs...)
	}
	if err != nil {
		e := Err{Fix: "LIVEDB:error executing query:{{.Query}}",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Query", s},
			},
		}
		return 0, fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}
	defer rows.Close()

	if !rows.Next() {
		err = rows.Err()
		if err != nil {
			e := Err{Fix: "LIVEDB:error at rows.Next for query:{{.Query}}",
				Var: []struct {
					Name  string
					Value interface{}
				}{
					{"Query", s},
				},
			}
			return 0, fmt.Errorf(fnc+":%w:"+err.Error(), e)
		}
		return 0, nil
	}

	var n int
	err = rows.Scan(&n)
	if err != nil {
		e := Err{Fix: "LIVEDB:error at scan(rows) for query:{{.Query}}",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Query", s},
			},
		}
		return 0, fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	return n, nil
}

// Table.ByKey returns a Record and possibly an error.
func (t *Table) ByKey(key int, tx *sql.Tx, opts ...func(*Table)) ([]Record, error) {
	fnc := "Table.ByKey"

	for _, opt := range opts { // non-default options
		opt(t)
	}

	err := t.byKeyPrecs(key) // preconditions
	if err != nil {
		return []Record{}, fmt.Errorf(fnc+":%w", err)
	}

	recs, err := t.byKey(key, tx)
	if err != nil {
		return []Record{}, fmt.Errorf(fnc+":%w", err)
	}

	return recs, nil
}

func (t *Table) byKeyPrecs(key int) error {
	fnc := "Table.byKeyPrecs"

	if GDb == nil {
		err := Err{Fix: "LIVEDB:access needs at least database object"}
		return fmt.Errorf(fnc+":%w", err)
	}

	if key == 0 {
		err := Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "key"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}

	if t.Name == "" {
		err := Err{Fix: "LIVEDB:table name missing"}
		return fmt.Errorf(fnc+":%w", err)
	}
	if t.Atts == nil {
		err := Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "t.Atts"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}
	if t.Scan == nil {
		err := Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "t.Scan"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}

	return nil
}

func (t *Table) byKey(key int, tx *sql.Tx) ([]Record, error) {
	fnc := "Table.byKey"

	var buf bytes.Buffer
	var put = buf.WriteString // write method

	sqlargs := []interface{}{}
	num := int(0)

	put("select ")
	for i, att := range StdAtts {
		if i == 0 { // first one
			put(att)
			continue
		}
		put("," + att)
	}
	for _, att := range t.Atts {
		put("," + att)
	}
	put(" from " + t.Name)

	num++
	put(" where pkey=" + FormatAtt(num) + ";")
	sqlargs = append(sqlargs, fmt.Sprint(key))

	s := buf.String()

	Log("s:", s)
	Log("sqlargs:", sqlargs)

	recs, err := t.Query(s, sqlargs, t.Scan, tx)
	if err != nil {
		return []Record{}, fmt.Errorf(fnc+":%w", err)
	}

	return recs, nil
}

// Table.ByIDBegin returns a Record and possibly an error.
func (t *Table) ByIDBegin(id int, begin string, tx *sql.Tx, opts ...func(*Table)) ([]Record, error) {
	fnc := "Table.ByIDBegin"

	for _, opt := range opts { // non-default options
		opt(t)
	}

	err := t.byIDBeginPrecs(id, begin) // preconditions
	if err != nil {
		return []Record{}, fmt.Errorf(fnc+":%w", err)
	}

	recs, err := t.byIDBegin(id, begin, tx)
	if err != nil {
		return []Record{}, fmt.Errorf(fnc+":%w", err)
	}

	return recs, nil
}

func (t *Table) byIDBeginPrecs(id int, ts string) error {
	fnc := "Table.byIDBeginPrecs"

	if GDb == nil {
		err := Err{Fix: "LIVEDB:access needs at least database object"}
		return fmt.Errorf(fnc+":%w", err)
	}

	if id == 0 {
		err := Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "id"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}

	if ts == "" { // ts must be provided
		err := Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "ts"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}

	if t.Name == "" {
		err := Err{Fix: "LIVEDB:table name missing"}
		return fmt.Errorf(fnc+":%w", err)
	}

	if t.Scan == nil {
		err := Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "t.Scan"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}

	return nil
}

func (t *Table) byIDBegin(id int, begin string, tx *sql.Tx) ([]Record, error) {
	fnc := "Table.byIDBegin"

	var buf bytes.Buffer
	var put = buf.WriteString // write method

	sqlargs := []interface{}{}
	num := int(0)

	put("select ")
	for i, att := range StdAtts {
		if i == 0 { // first one
			put(att)
			continue
		}
		put("," + att)
	}
	for _, att := range t.Atts {
		put("," + att)
	}
	put(" from " + t.Name)

	num++
	put(" where id=" + FormatAtt(num))
	sqlargs = append(sqlargs, fmt.Sprint(id))
	if begin == Now {
		put(" and begin=" + FormatNow() + ";")
	} else {
		num++
		put(" and begin=" + FormatTmsp(num) + ";")
		sqlargs = append(sqlargs, begin)
	}

	s := buf.String()

	Log("s:", s)
	Log("sqlargs:", sqlargs)

	recs, err := t.Query(s, sqlargs, t.Scan, tx)
	if err != nil {
		return []Record{}, fmt.Errorf(fnc+":%w", err)
	}

	return recs, nil
}

// Table.ByIDUntil returns a Record and possibly an error.
func (t *Table) ByIDUntil(id int, until string, tx *sql.Tx, opts ...func(*Table)) ([]Record, error) {
	fnc := "Table.ByIDUntil"

	for _, opt := range opts { // non-default options
		opt(t)
	}

	err := t.byIDUntilPrecs(id, until) // preconditions
	if err != nil {
		return []Record{}, fmt.Errorf(fnc+":%w", err)
	}

	recs, err := t.byIDUntil(id, until, tx)
	if err != nil {
		return []Record{}, fmt.Errorf(fnc+":%w", err)
	}

	return recs, nil
}

func (t *Table) byIDUntilPrecs(id int, ts string) error {
	fnc := "Table.byIDUntilPrecs"

	if GDb == nil {
		err := Err{Fix: "LIVEDB:access needs at least database object"}
		return fmt.Errorf(fnc+":%w", err)
	}

	if id == 0 {
		err := Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "id"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}

	if ts == "" { // ts must be provided
		err := Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "ts"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}

	if t.Name == "" {
		err := Err{Fix: "LIVEDB:table name missing"}
		return fmt.Errorf(fnc+":%w", err)
	}

	if t.Scan == nil {
		err := Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "t.Scan"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}

	return nil
}

func (t *Table) byIDUntil(id int, until string, tx *sql.Tx) ([]Record, error) {
	fnc := "Table.byIDUntil"

	var buf bytes.Buffer
	var put = buf.WriteString // write method

	sqlargs := []interface{}{}
	num := int(0)

	put("select ")
	for i, att := range StdAtts {
		if i == 0 { // first one
			put(att)
			continue
		}
		put("," + att)
	}
	for _, att := range t.Atts {
		put("," + att)
	}
	put(" from " + t.Name)

	num++
	put(" where id=" + FormatAtt(num))
	sqlargs = append(sqlargs, fmt.Sprint(id))
	if until == Now {
		put(" and until=" + FormatNow() + ";")
	} else {
		num++
		put(" and until=" + FormatTmsp(num) + ";")
		sqlargs = append(sqlargs, until)
	}

	s := buf.String()

	Log("s:", s)
	Log("sqlargs:", sqlargs)

	recs, err := t.Query(s, sqlargs, t.Scan, tx)
	if err != nil {
		return []Record{}, fmt.Errorf(fnc+":%w", err)
	}

	return recs, nil
}

// NameValue is a type used by function ByTsAndXs.
type NameValue struct {
	Name  string
	Value interface{}
}

// Table.ByTsAndXs returns Records and possibly an error.
// Results are ordered by all names of xs and ID and Begin.
func (t *Table) ByTsAndXs(ts string, xs []NameValue, tx *sql.Tx, opts ...func(*Table)) ([]Record, error) {
	fnc := "Table.ByTsAndXs"

	for _, opt := range opts { // non-default options
		opt(t)
	}

	err := t.byTsAndXsPrecs(ts, xs) // preconditions
	if err != nil {
		return []Record{}, fmt.Errorf(fnc+":%w", err)
	}

	recs, err := t.byTsAndXs(ts, xs, tx)
	if err != nil {
		return []Record{}, fmt.Errorf(fnc+":%w", err)
	}

	return recs, nil
}

func (t *Table) byTsAndXsPrecs(ts string, xs []NameValue) error {
	fnc := "Table.byTsAndXsPrecs"

	if GDb == nil {
		err := Err{Fix: "LIVEDB:access needs at least database object"}
		return fmt.Errorf(fnc+":%w", err)
	}

	if ts == "" { // ts must be provided
		err := Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "ts"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}

	if len(xs) == 0 {
		err := Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "xs"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}

	if t.Name == "" {
		err := Err{Fix: "LIVEDB:table name missing"}
		return fmt.Errorf(fnc+":%w", err)
	}

	if t.Scan == nil {
		err := Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "t.Scan"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}

	return nil
}

func (t *Table) byTsAndXs(ts string, xs []NameValue, tx *sql.Tx) ([]Record, error) {
	fnc := "Table.byTsAndXs"

	var buf bytes.Buffer
	var put = buf.WriteString // write method

	sqlargs := []interface{}{}
	num := int(0)

	put("select ")
	for i, att := range StdAtts {
		if i == 0 { // first one
			put(att)
			continue
		}
		put("," + att)
	}
	for _, att := range t.Atts {
		put("," + att)
	}
	put(" from " + t.Name)

	if t.New.Std.Begin == Now {
		put(" where begin<=" + FormatNow())
	} else {
		num++
		put(" where begin<=" + FormatTmsp(num))
		sqlargs = append(sqlargs, ts)
	}
	num++
	put(" and (until is null or until>" + FormatTmsp(num) + ")")
	sqlargs = append(sqlargs, ts)
	for _, x := range xs {
		num++
		put(" and " + x.Name + "=" + FormatAtt(num))
		sqlargs = append(sqlargs, x.Value)
	}

	put(" order by ")
	for _, x := range xs {
		put(x.Name + ",")
	}
	put("id,begin;")

	s := buf.String()

	Log("s:", s)
	Log("sqlargs:", sqlargs)

	recs, err := t.Query(s, sqlargs, t.Scan, tx)
	if err != nil {
		return []Record{}, fmt.Errorf(fnc+":%w", err)
	}

	return recs, nil
}

// Table.ByTs returns Records and possibly an error.
func (t *Table) ByTs(ts string, tx *sql.Tx, opts ...func(*Table)) ([]Record, error) {
	fnc := "Table.ByTs"

	for _, opt := range opts { // non-default options
		opt(t)
	}

	err := t.byTsPrecs(ts) // preconditions
	if err != nil {
		return []Record{}, fmt.Errorf(fnc+":%w", err)
	}

	var xs []NameValue // empty
	recs, err := t.byTsAndXs(ts, xs, tx)
	if err != nil {
		return []Record{}, fmt.Errorf(fnc+":%w", err)
	}

	return recs, nil
}

func (t *Table) byTsPrecs(ts string) error {
	fnc := "Table.byTsPrecs"

	if GDb == nil {
		err := Err{Fix: "LIVEDB:access needs at least database object"}
		return fmt.Errorf(fnc+":%w", err)
	}

	if ts == "" { // ts must be provided
		err := Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "ts"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}

	if t.Name == "" {
		err := Err{Fix: "LIVEDB:table name missing"}
		return fmt.Errorf(fnc+":%w", err)
	}

	if t.Scan == nil {
		err := Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "t.Scan"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}

	return nil
}

// Table.ByIDTs returns a Record and possibly an error.
func (t *Table) ByIDTs(id int, ts string, tx *sql.Tx, opts ...func(*Table)) ([]Record, error) {
	fnc := "Table.ByIDTs"

	for _, opt := range opts { // non-default options
		opt(t)
	}

	err := t.byIDTsPrecs(id, ts) // preconditions
	if err != nil {
		return []Record{}, fmt.Errorf(fnc+":%w", err)
	}

	xs := []NameValue{{Name: "id", Value: id}}
	recs, err := t.byTsAndXs(ts, xs, tx)
	if err != nil {
		return []Record{}, fmt.Errorf(fnc+":%w", err)
	}

	return recs, nil
}

func (t *Table) byIDTsPrecs(id int, ts string) error {
	fnc := "Table.byIDTsPrecs"

	if GDb == nil {
		err := Err{Fix: "LIVEDB:access needs at least database object"}
		return fmt.Errorf(fnc+":%w", err)
	}

	if id == 0 {
		err := Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "id"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}

	if ts == "" { // ts must be provided
		err := Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "ts"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}

	if t.Name == "" {
		err := Err{Fix: "LIVEDB:table name missing"}
		return fmt.Errorf(fnc+":%w", err)
	}

	if t.Scan == nil {
		err := Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "t.Scan"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}

	return nil
}

func (t *Table) byXs(xs []NameValue, tx *sql.Tx) ([]Record, error) {
	fnc := "Table.byXs"

	var buf bytes.Buffer
	var put = buf.WriteString // write method

	sqlargs := []interface{}{}
	num := int(0)

	put("select ")
	for i, att := range StdAtts {
		if i == 0 { // first one
			put(att)
			continue
		}
		put("," + att)
	}
	for _, att := range t.Atts {
		put("," + att)
	}
	put(" from " + t.Name)

	for i, x := range xs {
		if i == 0 {
			put(" where ")
		} else {
			put(" and ")
		}
		num++
		put(x.Name + "=" + FormatAtt(num))
		sqlargs = append(sqlargs, x.Value)
	}

	put(" order by ")
	for _, x := range xs {
		put(x.Name + ",")
	}
	put("id,begin;")

	s := buf.String()

	Log("s:", s)
	Log("sqlargs:", sqlargs)

	recs, err := t.Query(s, sqlargs, t.Scan, tx)
	if err != nil {
		return []Record{}, fmt.Errorf(fnc+":%w", err)
	}

	return recs, nil
}

func (t *Table) Query(s string, sqlargs []interface{}, scan ScanFunc, tx *sql.Tx) ([]Record, error) {
	fnc := "Table.Query"

	rows := &sql.Rows{}
	var err error
	var recs []Record

	if tx != nil {
		rows, err = tx.Query(s, sqlargs...)
	} else {
		rows, err = GDb.Query(s, sqlargs...)
	}
	if err != nil {
		e := Err{Fix: "LIVEDB:error executing query:{{.Query}}",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Query", strings.ReplaceAll(s, "%", "_")}, // better for fmt.Errorf
			},
		}
		return []Record{}, fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}
	defer rows.Close()

	for rows.Next() {
		rec, err := t.Scan(rows) // call the given scan function
		if err != nil {
			e := Err{Fix: "LIVEDB:error at scan(rows) for query:{{.Query}}",
				Var: []struct {
					Name  string
					Value interface{}
				}{
					{"Query", strings.ReplaceAll(s, "%", "_")}, // better for fmt.Errorf
				},
			}
			return []Record{}, fmt.Errorf(fnc+":%w:"+err.Error(), e)
		}
		recs = append(recs, rec)
	}
	err = rows.Err()
	if err != nil {
		e := Err{Fix: "LIVEDB:error at rows.Next for query:{{.Query}}",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Query", strings.ReplaceAll(s, "%", "_")}, // better for fmt.Errorf
			},
		}
		return []Record{}, fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	return recs, nil
}
