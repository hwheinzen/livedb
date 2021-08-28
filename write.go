// Copyright 2021 Hans-Werner Heinzen. All rights reserved.
// Use of this source code is governed by a license
// that can be found in the LICENSE file.

package livedb

import (
	"bytes"
	"database/sql"
	"fmt"

	. "github.com/hwheinzen/stringl10n/mistake"
)

func handleTs(ts string) (string, error) {
	fnc := "handleTs"

	var err error
	var out string
	var past bool

	if ts == Now {
		return ts, nil // Now will be handled later
// 		out, err = CurrentTmsp(nil) // this usually happens earlier
// 		if err != nil {
// 			return "", fmt.Errorf(fnc+":%w", err)
// 		}
	} else {
		out, past, _, _, err = Tmsp(ts, nil) // valid and past?
		if err != nil {
			return "", fmt.Errorf(fnc+":%w", err)
		}
		if past {
			err := Err{
				Fix: "LIVEDB:cannot change the past: {{.Name}}",
				Var: []struct {
					Name  string
					Value interface{}
				}{
					{"Name", out},
				},
			}
			return "", fmt.Errorf(fnc+":%w", err)
		}
	}
	return out, nil
}

func writePrecs(ts, creator string, tx *sql.Tx) error {
	fnc := "writePrecs"

	var err error

	if ts == "" { // ts must be provided
		err = Err{
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
	if creator == "" { // creator must be provided
		err = Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "creator"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}
	if tx == nil { // tx must be provided
		err = Err{Fix: "LIVEDB:write access needs transaction object"}
		return fmt.Errorf(fnc+":%w", err)
	}

	return nil
}

// Table.NewID a new row of the ID-table and returns the ID.
func (t *Table) NewID(creator string, tx *sql.Tx) (id int, err error) {
	fnc := "Table.NewID"

	err = t.newIDPrecs(creator, tx) // preconditions
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}

	id, err = t.newID(creator, tx)
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}

	return id, nil
}

func (t *Table) newIDPrecs(creator string, tx *sql.Tx) error {
	fnc := "Table.newIDPrecs"

	if tx == nil {
		err := Err{Fix: "LIVEDB:write access needs transaction object"}
		return fmt.Errorf(fnc+":%w", err)
	}
	if creator == "" {
		err := Err{
			Fix: "LIVEDB:{{.Name}} missing for {{.Table}}",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "creator"},
				{"Table", t.Name},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}
	if t.Name == "" {
		err := Err{Fix: "LIVEDB:table name missing"}
		return fmt.Errorf(fnc+":%w", err)
	}

	return nil
}

func (t *Table) newID(creator string, tx *sql.Tx) (int, error) {
	fnc := "Table.newID"

	var buf1, buf2 bytes.Buffer
	var put1 = buf1.WriteString // write method
	var put2 = buf2.WriteString // write method

	sqlargs := []interface{}{}
	num := int(0)

	put1("insert into " + t.Name + "id (")
	put2(") values (")

	put1("created,")
	put2(FormatNow() + ",")

	num++
	put1("createdby")
	put2(FormatAtt(num))
	sqlargs = append(sqlargs, creator)

	s := buf1.String() + buf2.String() + ");"

	Log("s:", s)
	Log("sqlargs:", sqlargs)

	var res sql.Result
	var err error

	if tx != nil {
		res, err = tx.Exec(s, sqlargs...)
	} else {
		res, err = GDb.Exec(s, sqlargs...)
	}
	if err != nil {
		e := Err{
			Fix: "LIVEDB:error inserting by:{{.Query}}",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Query", s},
			},
		}
		return 0, fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	var n int

	if tx != nil {
		n, err = t.insertedID(tx, res)
	} else {
		n, err = t.insertedID(nil, res)
	}
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}

	Log("new id:", n)

	return n, nil
}

func (t *Table) useID(creator string, tx *sql.Tx) error {
	fnc := "Table.useID"

	sqlargs := []interface{}{}
	num := int(0)

	var buf bytes.Buffer
	var put = buf.WriteString // write method

	put("update " + t.Name + "id set ")

	num++
	put("usedby=" + FormatAtt(num))
	sqlargs = append(sqlargs, creator)

	num++
	put(" where id=" + FormatAtt(num))
	sqlargs = append(sqlargs, fmt.Sprint(t.New.Std.ID))

	num++
	put(" and createdby=" + FormatAtt(num))
	sqlargs = append(sqlargs, creator)

	put(" and usedby is null;")

	s := buf.String()

	Log("s:", s)
	Log("sqlargs:", sqlargs)

	var res sql.Result
	var err error

	res, err = tx.Exec(s, sqlargs...)

	if err != nil {
		e := Err{Fix: "LIVEDB:error executing ID-table update"}
		return fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	n, err := res.RowsAffected()
	if err != nil {
		e := Err{Fix: "LIVEDB:error getting rows affected"}
		return fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}
	if n == 0 {
		err = Err{Fix: "LIVEDB:ID-table not updated"}
		return fmt.Errorf(fnc+":%w", err)
	}

	return nil
}

// Start creates a new object, inserts its first record and returns
// the primary key of this record.
func (t *Table) Start(id int, ts, creator string, tx *sql.Tx) (key int, err error) {
	fnc := "Table.Start"

	err = t.startPrecs(id, ts, creator, tx) // preconditions
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}

	key, err = t.start(id, ts, creator, tx)
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}

	return key, nil
}

func (t *Table) startPrecs(id int, ts, creator string, tx *sql.Tx) error {
	fnc := "Table.startPrecs"

	err := writePrecs(ts, creator, tx)
	if err != nil {
		return fmt.Errorf(fnc+":%w", err)
	}

	if id == 0 { // id must be provided
		err = Err{
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

	if t.Name == "" { // t.Name must be provided
		err = Err{Fix: "LIVEDB:table name missing"}
		return fmt.Errorf(fnc+":%w", err)
	}
	if t.New.Idv == nil { // t.New.Idv must be provided
		err = Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "t.NewIdv"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}
	if t.Atts == nil { // t.Atts must be provided
		err = Err{
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
	if t.Vals == nil { // t.Vals must be provided
		err = Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "t.Vals"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}

	return nil
}

func (t *Table) start(id int, ts, creator string, tx *sql.Tx) (int, error) {
	fnc := "Table.start"

	var err error

	// TODO: check if id is already used

	ts, err = handleTs(ts)
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}

	t.New.Std = Std{
		ID:        id,
		Begin:     ts,
		CreatedBy: creator,
	}

	key, err := t.ins(tx) // <-- ACTION INSERT
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}

	err = t.useID(creator, tx) // mark as used
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}

	return key, nil
}

// Terminate terminates a given record with Until = ts.
func (t *Table) Terminate(ts, terminator string, tx *sql.Tx) (key int, err error) {
	fnc := "Table.Terminate"

	err = t.terminatePrecs(ts, terminator, tx) // preconditions
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}

	key, err = t.terminate(ts, terminator, tx)
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}

	return key, nil
}

func (t *Table) terminatePrecs(ts, terminator string, tx *sql.Tx) error {
	fnc := "Table.terminatePrecs"

	err := writePrecs(ts, terminator, tx)
	if err != nil {
		return fmt.Errorf(fnc+":%w", err)
	}

	if t.Name == "" { // t.Name must be provided
		err = Err{Fix: "LIVEDB:table name missing"}
		return fmt.Errorf(fnc+":%w", err)
	}
	if t.Old.Std.Pkey == 0 { // t.Old.Std must be provided
		err = Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "t.Old.Std"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}
	if t.Old.Idv == nil { // t.Old.Idv must be provided
		err = Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "t.Old.Idv"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}
	if t.Atts == nil { // t.Atts must be provided
		err = Err{
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
	if t.Scan == nil { // t.Scan must be provided
		err = Err{
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

func (t *Table) terminate(ts, terminator string, tx *sql.Tx) (int, error) {
	fnc := "Table.terminate"

	var key = t.Old.Std.Pkey
	var err error

	ts, err = handleTs(ts)
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}
	if ts == t.Old.Std.Until {
		return t.Old.Std.Pkey, nil // NO CHANGE
	}
	if ts < t.Old.Std.Begin {
		err = Err{Fix: "LIVEDB:not allowed"}
		return 0, fmt.Errorf(fnc+":%w", err)
	}

	sames, err := t.byKey(t.Old.Std.Pkey, tx)
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}
	same := sames[0]
	if same.Std.Pkey == 0 {
		err = Err{Fix: "LIVEDB:competetively deleted"}
		return 0, fmt.Errorf(fnc+":%w", err)
	}
	if t.Old.Idv != same.Idv || t.Old.Std != same.Std {
		err = Err{Fix: "LIVEDB:competetively changed"}
		return 0, fmt.Errorf(fnc+":%w", err)
	}

	if ts == t.Old.Std.Begin {
		t.New.Std = t.Old.Std
		err = t.del(tx) // <-- ACTION: delete
		if err != nil {
			return 0, fmt.Errorf(fnc+":%w", err)
		}
		key = 0
	} else {
		t.New.Std = t.Old.Std
		t.New.Std.Until = ts
		t.New.Std.EndedBy = terminator
		err = t.until(tx) // <-- ACTION: update Until
		if err != nil {
			return 0, fmt.Errorf(fnc+":%w", err)
		}
	}

	if t.Old.Std.Until != "" {
		nexts, err := t.byIDBegin(t.Old.Std.ID, t.Old.Std.Until, tx) // read follower
		if err != nil {
			return 0, fmt.Errorf(fnc+":%w", err)
		}
		next := nexts[0]
		if next.Std.Pkey == 0 { // no follower: work DONE
			return key, nil
		}
		for next.Std.Pkey != 0 {
			t.New.Std = next.Std
			err = t.del(tx) // <-- ACTION delete all followers
			if err != nil {
				return 0, fmt.Errorf(fnc+":%w", err)
			}
			if next.Std.Until != "" {
				nexts, err = t.byIDBegin(next.Std.ID, next.Std.Until, tx) // next
				if err != nil {
					return 0, fmt.Errorf(fnc+":%w", err)
				}
				next = nexts[0]
			} else {
				next.Std = Std{}
			}
		}
	}

	return key, nil
}

// Change applies changes to a given record belonging to a specific ID and returns
// the primary key of the changed record.
//
// It usually creates a new record and updates the Until attribute of a predecessor.
// It may only update the given (future) record.
// It evantually creates a new record with a new ID.
func (t *Table) Change(ts, creator string, tx *sql.Tx, opts ...func(*Table)) (key int, err error) {
	fnc := "Table.Change"

	for _, opt := range opts { // non-default options
		opt(t)
	}

	err = t.changePrecs(ts, creator, tx) // preconditions
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}

	key, err = t.change(ts, creator, tx)
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}

	return key, nil
}

func (t *Table) changePrecs(ts, creator string, tx *sql.Tx) error {
	fnc := "Table.changePrecs"

	err := writePrecs(ts, creator, tx)
	if err != nil {
		return fmt.Errorf(fnc+":%w", err)
	}

	if t.Name == "" { // t.Name must be provided
		err = Err{Fix: "LIVEDB:table name missing"}
		return fmt.Errorf(fnc+":%w", err)
	}
	if t.Old.Std.Pkey == 0 { // t.Old.Std must be provided
		err = Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "t.Old.Std"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}
	if t.Old.Idv == nil { // t.Old.Idv must be provided
		err = Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "t.Old.Idv"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}
	if t.New.Idv == nil { // t.New.Idv must be provided
		err = Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "t.New.Idv"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}
	if t.Atts == nil { // t.Atts must be provided
		err = Err{
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
	if t.Vals == nil { // t.Vals must be provided
		err = Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "t.Vals"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}
	if t.Scan == nil { // t.Scan must be provided
		err = Err{
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

func (t *Table) change(ts, creator string, tx *sql.Tx) (int, error) {
	fnc := "Table.change"

	var err error

	ts, err = handleTs(ts)
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}
	
	if t.New.Idv == t.Old.Idv {
		return t.Old.Std.Pkey, nil // NOTHING CHANGED
	}

	sames, err := t.byKey(t.Old.Std.Pkey, tx)
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}
	same := sames[0]
	if same.Std.Pkey == 0 {
		err = Err{Fix: "LIVEDB:competetively deleted"}
		return 0, fmt.Errorf(fnc+":%w", err)
	}
	if t.Old.Idv != same.Idv || t.Old.Std != same.Std {
		err = Err{Fix: "LIVEDB:competetively changed"}
		return 0, fmt.Errorf(fnc+":%w", err)
	}

	if ts == t.Old.Std.Begin {

		t.New.Std = t.Old.Std
		t.New.Std.CreatedBy = creator
		err = t.upd(tx) // <-- ACTION UPDATE
		if err != nil {
			return 0, fmt.Errorf(fnc+":%w", err)
		}
		return t.New.Std.Pkey, nil

	} else {

		t.New.Std = Std{
			ID:        t.Old.Std.ID,
			Begin:     ts,
			CreatedBy: creator,
		}
		key, err := t.ins(tx) // <-- ACTION INSERT
		if err != nil {
			return 0, fmt.Errorf(fnc+":%w", err)
		}

		t.New.Std = t.Old.Std
		t.New.Std.Until = ts
		t.New.Std.EndedBy = creator
		err = t.until(tx) //  <-- ACTION UPDATE until
		if err != nil {
			return 0, fmt.Errorf(fnc+":%w", err)
		}

		return key, nil
	}
}

// MoveBegin begins a given record with Begin = ts.
func (t *Table) MoveBegin(ts, creator string, tx *sql.Tx) (key int, err error) {
	fnc := "Table.MoveBegin"

	err = t.moveBeginPrecs(ts, creator, tx) // preconditions
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}

	key, err = t.moveBegin(ts, creator, tx)
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}

	return key, nil
}

func (t *Table) moveBeginPrecs(ts, creator string, tx *sql.Tx) error {
	fnc := "Table.moveBeginPrecs"

	err := writePrecs(ts, creator, tx)
	if err != nil {
		return fmt.Errorf(fnc+":%w", err)
	}

	if t.Name == "" { // t.Name must be provided
		err = Err{Fix: "LIVEDB:table name missing"}
		return fmt.Errorf(fnc+":%w", err)
	}
	if t.Old.Std.Pkey == 0 { // t.Old.Std must be provided
		err = Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "t.Old.Std"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}
	if t.Old.Idv == nil { // t.Old.Idv must be provided
		err = Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "t.Old.Idv"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}
	if t.Atts == nil { // t.Atts must be provided
		err = Err{
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
	if t.Scan == nil { // t.Scan must be provided
		err = Err{
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

func (t *Table) moveBegin(ts, creator string, tx *sql.Tx) (int, error) {
	fnc := "Table.moveBegin"

	var key = t.Old.Std.Pkey
	var err error

	ts, err = handleTs(ts)
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}
	if ts == t.Old.Std.Begin {
		return t.Old.Std.Pkey, nil // NO CHANGE
	}
	if ts > t.Old.Std.Until {
		err = Err{Fix: "LIVEDB:not allowed"}
		return 0, fmt.Errorf(fnc+":%w", err)
	}

	sames, err := t.byKey(t.Old.Std.Pkey, tx)
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}
	same := sames[0]
	if same.Std.Pkey == 0 {
		err = Err{Fix: "LIVEDB:competetively deleted"}
		return 0, fmt.Errorf(fnc+":%w", err)
	}
	if t.Old.Idv != same.Idv || t.Old.Std != same.Std {
		err = Err{Fix: "LIVEDB:competetively changed"}
		return 0, fmt.Errorf(fnc+":%w", err)
	}

	if ts == t.Old.Std.Until {
		t.New.Std = t.Old.Std
		err = t.del(tx) // <-- ACTION: delete
		if err != nil {
			return 0, fmt.Errorf(fnc+":%w", err)
		}
		key = 0
	} else {
		t.New.Std = t.Old.Std
		t.New.Std.Begin = ts
		t.New.Std.EndedBy = creator
		err = t.begin(tx) // <-- ACTION: update Begin
		if err != nil {
			return 0, fmt.Errorf(fnc+":%w", err)
		}
	}

	nexts, err := t.byIDUntil(t.Old.Std.ID, t.Old.Std.Begin, tx) // read preceder
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}
	if len(nexts) == 0 { // no preceder: work DONE
		return key, nil
	}
	next := nexts[0]
	if ts < t.Old.Std.Begin {
		for len(nexts) != 0 && next.Std.Begin > ts {
			t.New.Std = next.Std
			err = t.del(tx) // <-- ACTION: DELETE shadowed preceder
			if err != nil {
				return 0, fmt.Errorf(fnc+":%w", err)
			}
			nexts, err = t.byIDUntil(next.Std.ID, next.Std.Begin, tx) // next
			if err != nil {
				return 0, fmt.Errorf(fnc+":%w", err)
			}
			if len(nexts) != 0 {
				next = nexts[0]
			}
		}
	}
	t.New.Std = next.Std
	t.New.Std.Until = ts
	t.New.Std.CreatedBy = creator
	err = t.until(tx) //  <-- ACTION: UPDATE preceder's until
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}

	return key, nil
}

// MoveUntil ends a given record with Until = ts.
func (t *Table) MoveUntil(ts, creator string, tx *sql.Tx) (key int, err error) {
	fnc := "Table.MoveUntil"

	err = t.moveUntilPrecs(ts, creator, tx) // preconditions
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}

	key, err = t.moveUntil(ts, creator, tx)
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}

	return key, nil
}

func (t *Table) moveUntilPrecs(ts, creator string, tx *sql.Tx) error {
	fnc := "Table.moveUntilPrecs"

	err := writePrecs(ts, creator, tx)
	if err != nil {
		return fmt.Errorf(fnc+":%w", err)
	}

	if t.Name == "" { // t.Name must be provided
		err = Err{Fix: "LIVEDB:table name missing"}
		return fmt.Errorf(fnc+":%w", err)
	}
	if t.Old.Std.Pkey == 0 { // t.Old.Std must be provided
		err = Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "t.Old.Std"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}
	if t.Old.Idv == nil { // t.Old.Idv must be provided
		err = Err{
			Fix: "LIVEDB:{{.Name}} missing",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{"Name", "t.Old.Idv"},
			},
		}
		return fmt.Errorf(fnc+":%w", err)
	}
	if t.Atts == nil { // t.Atts must be provided
		err = Err{
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
	if t.Scan == nil { // t.Scan must be provided
		err = Err{
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

func (t *Table) moveUntil(ts, creator string, tx *sql.Tx) (int, error) {
	fnc := "Table.moveUntil"

	var key = t.Old.Std.Pkey
	var err error

	ts, err = handleTs(ts)
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}
	if ts == t.Old.Std.Until {
		return t.Old.Std.Pkey, nil // NO CHANGE
	}
	if ts < t.Old.Std.Begin {
		err = Err{Fix: "LIVEDB:not allowed"}
		return 0, fmt.Errorf(fnc+":%w", err)
	}

	sames, err := t.byKey(t.Old.Std.Pkey, tx)
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}
	same := sames[0]
	if same.Std.Pkey == 0 {
		err = Err{Fix: "LIVEDB:competetively deleted"}
		return 0, fmt.Errorf(fnc+":%w", err)
	}
	if t.Old.Idv != same.Idv || t.Old.Std != same.Std {
		err = Err{Fix: "LIVEDB:competetively changed"}
		return 0, fmt.Errorf(fnc+":%w", err)
	}

	if ts == t.Old.Std.Begin {
		t.New.Std = t.Old.Std
		err = t.del(tx) // <-- ACTION: delete
		if err != nil {
			return 0, fmt.Errorf(fnc+":%w", err)
		}
		key = 0
	} else {
		t.New.Std = t.Old.Std
		t.New.Std.Until = ts
		t.New.Std.EndedBy = creator
		err = t.until(tx) // <-- ACTION: update Until
		if err != nil {
			return 0, fmt.Errorf(fnc+":%w", err)
		}
	}

	if t.Old.Std.Until != "" {
		nexts, err := t.byIDBegin(t.Old.Std.ID, t.Old.Std.Until, tx) // read follower
		if err != nil {
			return 0, fmt.Errorf(fnc+":%w", err)
		}
		if len(nexts) == 0 { // no follower: work DONE
			return key, nil
		}
		next := nexts[0]
		if ts > t.Old.Std.Until {
			for len(nexts) != 0 && next.Std.Until != "" && next.Std.Until < ts {
				t.New.Std = next.Std
				err = t.del(tx) // <-- ACTION: delete shadowed followers
				if err != nil {
					return 0, fmt.Errorf(fnc+":%w", err)
				}
				if next.Std.Until != "" {
					nexts, err = t.byIDBegin(next.Std.ID, next.Std.Until, tx) // next
					if err != nil {
						return 0, fmt.Errorf(fnc+":%w", err)
					}
					if len(nexts) != 0 {
						next = nexts[0]
					}
				}
			}
		}
		t.New.Std = next.Std
		t.New.Std.Begin = ts
		t.New.Std.CreatedBy = creator
		err = t.begin(tx) // <-- ACTION: update follower's begin
		if err != nil {
			return 0, fmt.Errorf(fnc+":%w", err)
		}
	}

	return key, nil
}

func (t *Table) ins(tx *sql.Tx) (int, error) {
	fnc := "Table.ins"

	vals := t.Vals(t.New.Idv)

	var buf1, buf2 bytes.Buffer
	var put1 = buf1.WriteString // write method
	var put2 = buf2.WriteString // write method

	sqlargs := []interface{}{}
	num := int(0)

	put1("insert into " + t.Name + " (")
	put2(") values (")

	// standard atts
	if t.New.Std.ID != 0 {
		num++
		put1("id,")
		put2(FormatAtt(num) + ",")
		sqlargs = append(sqlargs, t.New.Std.ID)
	}
	if t.New.Std.Begin != "" {
		if t.New.Std.Begin == Now {
			put1("begin,")
			put2(FormatNow() + ",")
		} else {
			num++
			put1("begin,")
			put2(FormatTmsp(num) + ",")
			sqlargs = append(sqlargs, t.New.Std.Begin)
		}
	}
	if t.New.Std.Until != "" {
		num++
		put1("until,")
		put2(FormatTmsp(num) + ",")
		sqlargs = append(sqlargs, t.New.Std.Until)
	}

	put1("created,")
	put2(FormatNow() + ",")

	if t.New.Std.CreatedBy != "" {
		num++
		put1("createdby,")
		put2(FormatAtt(num) + ",")
		sqlargs = append(sqlargs, t.New.Std.CreatedBy)
	}
	if t.New.Std.Ended != "" {
		num++
		put1("ended,")
		put2(FormatTmsp(num) + ",")
		sqlargs = append(sqlargs, t.New.Std.Ended)
	}
	if t.New.Std.EndedBy != "" {
		num++
		put1("endedby,")
		put2(FormatAtt(num) + ",")
		sqlargs = append(sqlargs, t.New.Std.EndedBy)
	}

	// table specific atts
	for i, att := range t.Atts {
		if vals[i] != "" { // otherwise NULL
			num++
			put1(att + ",")
			put2(FormatAtt(num) + ",")
			sqlargs = append(sqlargs, vals[i])
		}
	}

	// buffers without trailing commas
	s := buf1.String()[:buf1.Len()-1] + buf2.String()[:buf2.Len()-1] + ");"

	Log("s:", s)
	Log("sqlargs:", sqlargs)

	r, err := tx.Exec(s, sqlargs...)
	if err != nil {
		e := Err{Fix: "LIVEDB:error executing insert"}
		return 0, fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	key, err := t.insertedKey(tx, r)
	if err != nil {
		return 0, fmt.Errorf(fnc+":%w", err)
	}

	Log("inserted key:", key)

	return key, nil
}

func (t *Table) upd(tx *sql.Tx) error {
	fnc := "Table.upd"

	vals := t.Vals(t.New.Idv)

	sqlargs := []interface{}{}
	num := int(0)

	var buf bytes.Buffer
	var put = buf.WriteString // write method

	put("update " + t.Name + " set ")

	put("created=" + FormatNow())
	if t.New.Std.CreatedBy != "" {
		num++
		put(",createdby=" + FormatAtt(num))
		sqlargs = append(sqlargs, t.New.Std.CreatedBy)
	}

	// table specific atts
	for i, att := range t.Atts {
		if vals[i] != "" {
			num++
			put("," + att + "=" + FormatAtt(num))
			sqlargs = append(sqlargs, vals[i])
		} else {
			put("," + att + "=" + FormatNull())
		}
	}

	num++
	put(" where pkey=" + FormatAtt(num) + ";")
	sqlargs = append(sqlargs, fmt.Sprint(t.New.Std.Pkey))

	s := buf.String()

	Log("s:", s)
	Log("sqlargs:", sqlargs)

	var r sql.Result
	r, err := tx.Exec(s, sqlargs...)
	if err != nil {
		e := Err{Fix: "LIVEDB:error executing update"}
		return fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	n, err := r.RowsAffected()
	if err != nil {
		e := Err{Fix: "LIVEDB:error getting rows affected"}
		return fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}
	if n == 0 {
		err = Err{Fix: "LIVEDB:nothing updated"}
		return fmt.Errorf(fnc+":%w", err)
	}

	return nil
}

func (t *Table) del(tx *sql.Tx) error {
	fnc := "Table.del"

	var buf bytes.Buffer
	var put = buf.WriteString // write method

	sqlargs := []interface{}{}
	num := int(0)

	put("delete from " + t.Name)

	num++
	put(" where pkey=" + FormatAtt(num) + ";")
	sqlargs = append(sqlargs, fmt.Sprint(t.New.Std.Pkey))

	s := buf.String()

	Log("s:", s)
	Log("sqlargs:", sqlargs)

	var r sql.Result
	r, err := tx.Exec(s, sqlargs...)
	if err != nil {
		e := Err{Fix: "LIVEDB:error executing delete"}
		return fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	n, err := r.RowsAffected()
	if err != nil {
		e := Err{Fix: "LIVEDB:error getting rows affected"}
		return fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}
	if n == 0 {
		err = Err{Fix: "LIVEDB:nothing deleted"}
		return fmt.Errorf(fnc+":%w", err)
	}

	return nil
}

func (t *Table) begin(tx *sql.Tx) error {
	fnc := "Table.begin"

	var buf bytes.Buffer
	var put = buf.WriteString // write method

	sqlargs := []interface{}{}
	num := int(0)

	put("update " + t.Name + " set ")

	if t.New.Std.Begin == Now {
		put("begin=" + FormatNow() + ",")
	} else {
		num++
		put("begin=" + FormatTmsp(num) + ",")
		sqlargs = append(sqlargs, t.New.Std.Begin)
	}

	put("created=" + FormatNow() + ",")

	num++
	put("createdby=" + FormatAtt(num))
	sqlargs = append(sqlargs, t.New.Std.CreatedBy)

	num++
	put(" where pkey=" + FormatAtt(num) + ";")
	sqlargs = append(sqlargs, fmt.Sprint(t.New.Std.Pkey))

	s := buf.String()

	Log("s:", s)
	Log("sqlargs:", sqlargs)

	var r sql.Result
	r, err := tx.Exec(s, sqlargs...)
	if err != nil {
		e := Err{Fix: "LIVEDB:error executing update"}
		return fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	n, err := r.RowsAffected()
	if err != nil {
		e := Err{Fix: "LIVEDB:error getting rows affected"}
		return fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}
	if n == 0 {
		err = Err{Fix: "LIVEDB:nothing updated"}
		return fmt.Errorf(fnc+":%w", err)
	}

	return nil
}

func (t *Table) until(tx *sql.Tx) error {
	fnc := "Table.until"

	var buf bytes.Buffer
	var put = buf.WriteString // write method

	sqlargs := []interface{}{}
	num := int(0)

	put("update " + t.Name + " set ")

	if t.New.Std.Until == Now {
		put("until=" + FormatNow() + ",")
	} else {
		num++
		put("until=" + FormatTmsp(num) + ",")
		sqlargs = append(sqlargs, t.New.Std.Until)
	}

	put("ended=" + FormatNow() + ",")

	num++
	put("endedby=" + FormatAtt(num))
	sqlargs = append(sqlargs, t.New.Std.EndedBy)

	num++
	put(" where pkey=" + FormatAtt(num) + ";")
	sqlargs = append(sqlargs, fmt.Sprint(t.New.Std.Pkey))

	s := buf.String()

	Log("s:", s)
	Log("sqlargs:", sqlargs)

	var r sql.Result
	r, err := tx.Exec(s, sqlargs...)
	if err != nil {
		e := Err{Fix: "LIVEDB:error executing update"}
		return fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	n, err := r.RowsAffected()
	if err != nil {
		e := Err{Fix: "LIVEDB:error getting rows affected"}
		return fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}
	if n == 0 {
		err = Err{Fix: "LIVEDB:nothing updated"}
		return fmt.Errorf(fnc+":%w", err)
	}

	return nil
}
