// Copyright 2021 Hans-Werner Heinzen. All rights reserved.
// Use of this source code is governed by a license
// that can be found in the LICENSE file.

// Tests other than date/time tests.

package livedb

import (
	"database/sql"
	"fmt"
	"log"
	"testing"
	"time"

	. "github.com/hwheinzen/stringl10n/mistake"
)

//e = errors.New("ENTWICKLERTEST") // TEST - copy to appropriate location

//var lang = "de" // global for all error messages
var lang = "en" // global for all error messages

// TestMain ensures that database gDb is empty when tests start.
// It can be inspected afterwards.
func TestMain(m *testing.M) {
	var err error

	log.SetFlags(log.Ldate | log.Ltime) // start like prefix in testing.T

	err = removeDb() // cleanup before, not after!

	err = Open(gDbOpen) // connect to db
	if err != nil {
		err = translate(err, lang) // ******** l10n ********
		log.Fatal(err)
	}
	defer func() {
		err = Close() // disconnect from db
		if err != nil {
			err = translate(err, lang) // ******** l10n ********
			log.Fatal(err)
		}
	}()
	err = GDb.Ping() // test connection
	if err != nil {
		err = translate(err, lang) // ******** l10n ********
		log.Fatal(err)
	}

	t := Table{Name: tetab, Defs: teDefs}
	err = t.Create(nil) // prepare test table
	if err != nil {
		err = translate(err, lang) // ******** l10n ********
		log.Fatal(err)
	}

	m.Run() // run specified (or all) tests
}

const tetab = "ttest"

var teAtts = []string{
	"str1",
	"str2",
	"num",
}

var teDefs = []string{
	"str1 varchar(10)",
	"str2 varchar(10) not null",
	"num integer",
}

type TEID int

type Te struct {
//	ID   TEID
	str1 string
	str2 string
	num  int
}

type TestRec struct {
	Std // eingebettet
	Te  // eingebettet
}

func teScan(rows *sql.Rows) (Record, error) {
	fnc := "teScan"

	std := Std{}
	nullStr1 := sql.NullString{}
	nullStr2 := sql.NullString{}
	nullStr3 := sql.NullString{}

	te := Te{}
	nullStr4 := sql.NullString{}
	nullInt1 := sql.NullInt64{}

	err := rows.Scan(
		&(std.ID),
		&(std.Begin),
		&(nullStr1), // Until
		&(std.Pkey),
		&(std.Created), &(std.CreatedBy),
		&(nullStr2), &(nullStr2), // Ended, EndedBy
		//
		&(nullStr4), // str1
		&(te.str2),
		&(nullInt1), // num
	)
	if err != nil {
		e := Err{Fix: "LIVEDB:error scanning row}"}
		return Record{}, fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	if nullStr1.Valid {
		std.Until = nullStr1.String
	}
	if nullStr2.Valid {
		std.Ended = nullStr2.String
	}
	if nullStr3.Valid {
		std.EndedBy = nullStr3.String
	}

	if nullStr4.Valid {
		te.str1 = nullStr4.String
	}
	if nullInt1.Valid {
		te.num = int(nullInt1.Int64)
	}

//	te.ID = TEID(std.ID)

	return Record{Std: std, Idv: te}, nil
}

func teVals(in interface{}) []string {
	//	fnc := "teVals"

	te := in.(Te)

	vals := make([]string, len(teAtts))
	vals[0] = te.str1
	vals[1] = te.str2
	vals[2] = fmt.Sprintf("%0.d", te.num) // "" indicates NULL

	return vals
}

// TestNewID passes when two  calls to newID() return two different IDs.
func TestNewID(t *testing.T) {

	tab := Table{Name: tetab}
	creator := "TestNewID"

	tx, err := Begin() // begin transaction
	if err != nil {
		err = translate(err, lang) // ******** l10n ********
		t.Error(err)
	}
	defer func() {
		// don't rollback, check test database instaed
		// if err != nil {
		// 	e := Rollback(tx) // abort transaction
		// 	if e != nil {
		// 		e = translate(e, lang) // ******** l10n ********
		// 		t.Log(e)
		// 	}
		// 	return
		// }
		e := Commit(tx) // end transaction
		if e != nil {
			e = translate(e, lang) // ******** l10n ********
			t.Log(e)
		}
	}()

	id1, err := tab.newID(creator, tx)
	if err != nil {
		err = translate(err, lang) // ******** l10n ********
		t.Error(err)
	}

	id2, err := tab.newID(creator, tx)
	if err != nil {
		err = translate(err, lang) // ******** l10n ********
		t.Error(err)
	}

	if id1 == id2 {
		t.Errorf("expected two different IDs, got:  %d and %d\n", id1, id2)
	}
}

// TestUseID tests sucessful and unsuccesful calls to Table,useID.
func TestUseID(t *testing.T) {

	tab := Table{Name: tetab}
	creator := "TestUseID"
	type useIDTest struct {
		creator string
		ok      bool
	}
	useIDTests := []useIDTest{ // tests 1-3: don't change this sequence
		{"Wrong", false}, // 1 wrong creator
		{creator, true},  // 2 OK
		{creator, false}, // 3 already in use
	}

	tx, err := Begin() // begin transaction
	if err != nil {
		err = translate(err, lang) // ******** l10n ********
		t.Error(err)
	}
	defer func() {
		// don't rollback, check test database instaed
		// if err != nil {
		// 	e := Rollback(tx) // abort transaction
		// 	if e != nil {
		// 		e = translate(e, lang) // ******** l10n ********
		// 		t.Log(e)
		// 	}
		// 	return
		// }
		e := Commit(tx) // end transaction
		if e != nil {
			e = translate(e, lang) // ******** l10n ********
			t.Log(e)
		}
	}()

	id, err := tab.newID(creator, tx) // get new ID
	if err != nil {
		err = translate(err, lang) // ******** l10n ********
		t.Error(err)
	}
	tab.New.Std.ID = id

	for i, v := range useIDTests {
		err = tab.useID(v.creator, tx) // <------- ACTION
		switch {
		case !v.ok && err == nil:
			t.Error("#"+fmt.Sprintf("%d", i+1), "expected error, got ok")
		case v.ok && err != nil:
			err = translate(err, lang) // ******** l10n ********
			t.Error("#"+fmt.Sprintf("%d", i+1), "expected ok, got error:", err)
		case err != nil: // expected error
			err = translate(err, lang) // ******** l10n ********
			t.Log("#"+fmt.Sprintf("%d", i+1), "OK, error expected:", err)
		default:
			t.Log("#"+fmt.Sprintf("%d", i+1), "OK")
		}
	}
}

type startTest struct {
	ts   string
	str1 string
	str2 string
	num  int
	//
	ok bool
}

// startTests := []startTest{ are in livedb_sqlite|postgresql|mysql_test.go

// TestStart tests inserting records with new IDs.
func TestStart(t *testing.T) {

	creator := "TestStart"

	tx, err := Begin() // begin transaction
	if err != nil {
		err = translate(err, lang) // ******** l10n ********
		t.Error(err)
	}
	defer func() {
		// don't rollback, check test database instaed
		// if err != nil {
		// 	e := Rollback(tx) // abort transaction
		// 	if e != nil {
		// 		e = translate(e, lang) // ******** l10n ********
		// 		t.Log(e)
		// 	}
		// 	return
		// }
		e := Commit(tx) // end transaction
		if e != nil {
			e = translate(e, lang) // ******** l10n ********
			t.Log(e)
		}
	}()

	for i, v := range startTests {
		tab := Table{
			Name: tetab,
			Atts: teAtts,
			New: Record{
				Idv: Te{
					str1: v.str1,
					str2: v.str2,
					num:  v.num,
				},
			},
			Vals: teVals,
			Scan: teScan,
		}

		id, err := tab.newID(creator, tx) // get new ID
		if err != nil {
			err = translate(err, lang) // ******** l10n ********
			t.Error(err)
		}
		tab.New.Std.ID = id

		_, err = tab.Start(id, v.ts, creator, tx) // <------- ACTION
		switch {
		case !v.ok && err == nil:
			t.Error("#"+fmt.Sprintf("%d", i+1), "expected error, got ok")
		case v.ok && err != nil:
			err = translate(err, lang) // ******** l10n ********
			t.Error("#"+fmt.Sprintf("%d", i+1), "expected ok, got error:", err)
		case err != nil: // expected error
			err = translate(err, lang) // ******** l10n ********
			t.Log("#"+fmt.Sprintf("%d", i+1), "OK, error expected:", err)
		default:
			t.Log("#"+fmt.Sprintf("%d", i+1), "OK")
		}
	}
}

// TestStartNull tests inserting a record that violates NOT NULL.
// (This test is separate because Postgres needs a Rollback here.)
func TestStartNull(t *testing.T) {

	creator := "TestStartNull"

	type changeNullTest struct {
		ts   string
		str1 string
		str2 string // NOT NULL
		num  int
		//
		ok bool
	}
	changeNullTests := []changeNullTest{
		{Now, "0815String", "", 42, false}, // "" -> null
		// postgres does not allow commit after this error
	}

	tx, err := Begin() // begin transaction
	if err != nil {
		err = translate(err, lang) // ******** l10n ********
		t.Error(err)
	}
	defer func() {
		e := Rollback(tx) // abort transaction
		if e != nil {
			e = translate(e, lang) // ******** l10n ********
			t.Log(e)
		}
	}()

	for i, v := range changeNullTests {
		tab := Table{
			Name: tetab,
			Atts: teAtts,
			New: Record{
				Idv: Te{
					str1: v.str1,
					str2: v.str2,
					num:  v.num,
				},
			},
			Vals: teVals,
			Scan: teScan,
		}

		id, err := tab.newID(creator, tx) // get new ID
		if err != nil {
			err = translate(err, lang) // ******** l10n ********
			t.Error(err)
		}
		tab.New.Std.ID = id

		_, err = tab.Change(v.ts, creator, tx) // <------- ACTION
		switch {
		case !v.ok && err == nil:
			t.Error("#"+fmt.Sprintf("%d", i+1), "expected error, got ok")
		case v.ok && err != nil:
			err = translate(err, lang) // ******** l10n ********
			t.Error("#"+fmt.Sprintf("%d", i+1), "expected ok, got error:", err)
		case err != nil: // expected error
			err = translate(err, lang) // ******** l10n ********
			t.Log("#"+fmt.Sprintf("%d", i+1), "OK, error expected:", err)
		default:
			t.Log("#"+fmt.Sprintf("%d", i+1), "OK")
		}
	}
}

type changeTest struct {
	bTs   string
	bStr2 string
	bNum  int
	cTs   string
	cStr2 string
	cNum  int
	//
	ok bool
}

// changeTests := []changeTest{ are in livedb_sqlite|postgresql|mysql_test.go

// TestChange tests changing existing records by inserting a consecutive
// one, meaning: a) same ID and b) begin(New) = end(Old) with some other change.
func TestChange(t *testing.T) {

	creator := "TestChange"

	tx, err := Begin() // begin transaction
	if err != nil {
		err = translate(err, lang) // ******** l10n ********
		t.Error(err)
	}
	defer func() {
		// don't rollback, check test database instaed
		// if err != nil {
		// 	e := Rollback(tx) // abort transaction
		// 	if e != nil {
		// 		e = translate(e, lang) // ******** l10n ********
		// 		t.Log(e)
		// 	}
		// 	return
		// }
		e := Commit(tx) // end transaction
		if e != nil {
			e = translate(e, lang) // ******** l10n ********
			t.Log(e)
		}
	}()

	for i, v := range changeTests {

		tab := Table{
			Name: tetab,
			Atts: teAtts,
			New: Record{
				Idv: Te{
					str2: v.bStr2,
					num:  v.bNum,
				},
			},
			Vals: teVals,
			Scan: teScan,
		}

		id, err := tab.newID(creator, tx) // get new ID
		if err != nil {
			err = translate(err, lang) // ******** l10n ********
			t.Error(err)
		}
		tab.New.Std.ID = id

		key, err := tab.Start(id, v.bTs, creator, tx) // create the old one first ...
		if err != nil {
			err = translate(err, lang) // ******** l10n ********
			t.Error(err)
		}
		recs, err := tab.byKey(key, tx) // ... and read it
		if err != nil {
			err = translate(err, lang) // ******** l10n ********
			t.Error(err)
		}
		rec := recs[0]

		// define Old
		tab.Old = rec

		time.Sleep(1 * time.Millisecond) // make sure that time is running

		// changes
		tab.New = Record{
			Idv: Te{
				str2: v.cStr2,
				num:  v.cNum,
			},
		}

		_, err = tab.Change(v.cTs, creator, tx) // <------- ACTION
		switch {
		case !v.ok && err == nil:
			t.Error("#"+fmt.Sprintf("%d", i+1), "expected error, got ok")
		case v.ok && err != nil:
			err = translate(err, lang) // ******** l10n ********
			t.Error("#"+fmt.Sprintf("%d", i+1), "expected ok, got error:", err)
		case err != nil: // expected error
			err = translate(err, lang) // ******** l10n ********
			t.Log("#"+fmt.Sprintf("%d", i+1), "OK, error expected:", err)
		default:
			t.Log("#"+fmt.Sprintf("%d", i+1), "OK")
		}
	}
}

type terminateTest struct {
	bTs   string
	bStr2 string
	bNum  int
	//
	repeat   int
	addYears int
	//
	until string
	//
	ok bool
}

// terminateTests := []terminateTest{ are in livedb_sqlite|postgresql|mysql_test.go

// TestTerminate tests terminating existing records by setting Until.
func TestTerminate(t *testing.T) {

	creator := "TestTerminate"

	tx, err := Begin() // begin transaction
	if err != nil {
		err = translate(err, lang) // ******** l10n ********
		t.Error(err)
	}
	defer func() {
		// don't rollback, check test database instaed
		// if err != nil {
		// 	e := Rollback(tx) // abort transaction
		// 	if e != nil {
		// 		e = translate(e, lang) // ******** l10n ********
		// 		t.Log(e)
		// 	}
		// 	return
		// }
		e := Commit(tx) // end transaction
		if e != nil {
			e = translate(e, lang) // ******** l10n ********
			t.Log(e)
		}
	}()

	var id int
	for i, v := range terminateTests {

		tab := Table{
			Name: tetab,
			Atts: teAtts,
			New: Record{
				Idv: Te{
					str2: v.bStr2,
					num:  v.bNum,
				},
			},
			Vals: teVals,
			Scan: teScan,
		}

		id, err = tab.newID(creator, tx) // get new ID
		if err != nil {
			err = translate(err, lang) // ******** l10n ********
			t.Error(err)
		}
		tab.New.Std.ID = id

		key, err := tab.Start(id, v.bTs, creator, tx) // create the old one first ...
		if err != nil {
			err = translate(err, lang) // ******** l10n ********
			t.Error(err)
		}
		recs, err := tab.byKey(key, tx) // ... and read it
		if err != nil {
			err = translate(err, lang) // ******** l10n ********
			t.Error(err)
		}
		rec := recs[0]

		for i := v.repeat; i > 0; i-- {
			// define Old
			tab.Old = rec
			// new New
			v.bNum++
			tab.New = Record{
				Idv: Te{
					str2: (tab.Old.Idv).(Te).str2,
					num:  v.bNum,
				},
			}
			curDate, err := time.Parse(gTmspFormat, rec.Std.Begin)
			if err != nil {
				err = translate(err, lang) // ******** l10n ********
				t.Error(err)
			}
			begin := fmt.Sprint(curDate.Year()+v.addYears) + rec.Std.Begin[4:]
			key, err = tab.Change(begin, creator, tx) // changes
			if err != nil {
				err = translate(err, lang) // ******** l10n ********
				t.Error(err)
			}

			recs, err = tab.byKey(key, tx) // ... and read it
			if err != nil {
				err = translate(err, lang) // ******** l10n ********
				t.Error(err)
			}
			rec = recs[0]
		}

		xs := []NameValue{{Name: "id", Value: id}}
		recs, err = tab.byTsAndXs(v.until, xs, tx) // and read at v.until
		if err != nil {
			err = translate(err, lang) // ******** l10n ********
			t.Error(err)
		}
		rec = recs[0]

		tab.Old.Std = rec.Std
		tab.Old.Idv = rec.Idv

		_, err = tab.Terminate(v.until, creator, tx) // <------- ACTION
		switch {
		case !v.ok && err == nil:
			t.Error("#"+fmt.Sprintf("%d", i+1), "expected error, got ok")
		case v.ok && err != nil:
			err = translate(err, lang) // ******** l10n ********
			t.Error("#"+fmt.Sprintf("%d", i+1), "expected ok, got error:", err)
		case err != nil: // expected error
			err = translate(err, lang) // ******** l10n ********
			t.Log("#"+fmt.Sprintf("%d", i+1), "OK, error expected:", err)
		default:
			t.Log("#"+fmt.Sprintf("%d", i+1), "OK")
		}
	}
}

type moveUntilTest struct {
	bTs   string
	bStr2 string
	bNum  int
	//
	repeat   int
	addYears int
	//
	until string
	//
	ok bool
}

// moveUntilTests := []moveUntilTest{ are in livedb_sqlite|postgresql|mysql_test.go

// TestMoveUntil tests terminating existing records by setting Until.
func TestMoveUntil(t *testing.T) {

	creator := "TestMoveUntil"

	tx, err := Begin() // begin transaction
	if err != nil {
		err = translate(err, lang) // ******** l10n ********
		t.Error(err)
	}
	defer func() {
		// don't rollback, check test database instaed
		// if err != nil {
		// 	e := Rollback(tx) // abort transaction
		// 	if e != nil {
		// 		e = translate(e, lang) // ******** l10n ********
		// 		t.Log(e)
		// 	}
		// 	return
		// }
		e := Commit(tx) // end transaction
		if e != nil {
			e = translate(e, lang) // ******** l10n ********
			t.Log(e)
		}
	}()

	var id int
	for i, v := range moveUntilTests {

		tab := Table{
			Name: tetab,
			Atts: teAtts,
			New: Record{
				Idv: Te{
					str2: v.bStr2,
					num:  v.bNum,
				},
			},
			Vals: teVals,
			Scan: teScan,
		}

		id, err = tab.newID(creator, tx) // get new ID
		if err != nil {
			err = translate(err, lang) // ******** l10n ********
			t.Error(err)
		}
		tab.New.Std.ID = id

		key, err := tab.Start(id, v.bTs, creator, tx) // create the old one first ...
		if err != nil {
			err = translate(err, lang) // ******** l10n ********
			t.Error(err)
		}
		recs, err := tab.byKey(key, tx) // ... and read it
		if err != nil {
			err = translate(err, lang) // ******** l10n ********
			t.Error(err)
		}
		rec := recs[0]

		for i := v.repeat; i > 0; i-- {
			// define Old
			tab.Old = rec
			// new New
			v.bNum++
			tab.New = Record{
				Idv: Te{
					str2: (tab.Old.Idv).(Te).str2,
					num:  v.bNum,
				},
			}
			curDate, err := time.Parse(gTmspFormat, rec.Std.Begin)
			if err != nil {
				err = translate(err, lang) // ******** l10n ********
				t.Error(err)
			}
			begin := fmt.Sprint(curDate.Year()+v.addYears) + rec.Std.Begin[4:]
			key, err = tab.Change(begin, creator, tx) // changes
			if err != nil {
				err = translate(err, lang) // ******** l10n ********
				t.Error(err)
			}

			recs, err = tab.byKey(key, tx) // ... and read it
			if err != nil {
				err = translate(err, lang) // ******** l10n ********
				t.Error(err)
			}
			rec = recs[0]
		}

		xs := []NameValue{{Name: "id", Value: id}}
		recs, err = tab.byTsAndXs(v.until, xs, tx) // and read at v.until
		if err != nil {
			err = translate(err, lang) // ******** l10n ********
			t.Error(err)
		}
		rec = recs[0]

		tab.Old = rec

		_, err = tab.MoveUntil(v.until, creator, tx) // <------- ACTION
		switch {
		case !v.ok && err == nil:
			t.Error("#"+fmt.Sprintf("%d", i+1), "expected error, got ok")
		case v.ok && err != nil:
			err = translate(err, lang) // ******** l10n ********
			t.Error("#"+fmt.Sprintf("%d", i+1), "expected ok, got error:", err)
		case err != nil: // expected error
			err = translate(err, lang) // ******** l10n ********
			t.Log("#"+fmt.Sprintf("%d", i+1), "OK, error expected:", err)
		default:
			t.Log("#"+fmt.Sprintf("%d", i+1), "OK")
		}
	}
}

type moveBeginTest struct {
	bTs   string
	bStr2 string
	bNum  int
	//
	repeat   int
	addYears int
	//
	begin string
	//
	ok bool
}

// moveBeginTests := []moveBeginTest{ are in livedb_sqlite|postgresql|mysql_test.go

// TestMoveBegin tests terminating existing records by setting Begin.
func TestMoveBegin(t *testing.T) {

	creator := "TestMoveBegin"

	tx, err := Begin() // begin transaction
	if err != nil {
		err = translate(err, lang) // ******** l10n ********
		t.Error(err)
	}
	defer func() {
		// don't rollback, check test database instaed
		// if err != nil {
		// 	e := Rollback(tx) // abort transaction
		// 	if e != nil {
		// 		e = translate(e, lang) // ******** l10n ********
		// 		t.Log(e)
		// 	}
		// 	return
		// }
		e := Commit(tx) // end transaction
		if e != nil {
			e = translate(e, lang) // ******** l10n ********
			t.Log(e)
		}
	}()

	var id int
	for i, v := range moveBeginTests {

		tab := Table{
			Name: tetab,
			Atts: teAtts,
			New: Record{
				Idv: Te{
					str2: v.bStr2,
					num:  v.bNum,
				},
			},
			Vals: teVals,
			Scan: teScan,
		}

		id, err = tab.newID(creator, tx) // get new ID
		if err != nil {
			err = translate(err, lang) // ******** l10n ********
			t.Error(err)
		}
		tab.New.Std.ID = id

		key, err := tab.Start(id, v.bTs, creator, tx) // create the old one first ...
		if err != nil {
			err = translate(err, lang) // ******** l10n ********
			t.Error(err)
		}
		recs, err := tab.byKey(key, tx) // ... and read it
		if err != nil {
			err = translate(err, lang) // ******** l10n ********
			t.Error(err)
		}
		rec := recs[0]

		for i := v.repeat; i > 0; i-- {
			// define Old
			tab.Old = rec
			// new New
			v.bNum++
			tab.New = Record{
				Idv: Te{
					str2: (tab.Old.Idv).(Te).str2,
					num:  v.bNum,
				},
			}
			curDate, err := time.Parse(gTmspFormat, rec.Std.Begin)
			if err != nil {
				err = translate(err, lang) // ******** l10n ********
				t.Error(err)
			}
			begin := fmt.Sprint(curDate.Year()+v.addYears) + rec.Std.Begin[4:]
			key, err = tab.Change(begin, creator, tx) // changes
			if err != nil {
				err = translate(err, lang) // ******** l10n ********
				t.Error(err)
			}

			recs, err = tab.byKey(key, tx) // ... and read it
			if err != nil {
				err = translate(err, lang) // ******** l10n ********
				t.Error(err)
			}
			rec = recs[0]
		}

		xs := []NameValue{{Name: "id", Value: id}}
		recs, err = tab.byTsAndXs(v.begin, xs, tx) // and read at v.begin
		if err != nil {
			err = translate(err, lang) // ******** l10n ********
			t.Error(err)
		}
		rec = recs[0]

		tab.Old = rec

		_, err = tab.MoveBegin(v.begin, creator, tx) // <------- ACTION
		switch {
		case !v.ok && err == nil:
			t.Error("#"+fmt.Sprintf("%d", i+1), "expected error, got ok")
		case v.ok && err != nil:
			err = translate(err, lang) // ******** l10n ********
			t.Error("#"+fmt.Sprintf("%d", i+1), "expected ok, got error:", err)
		case err != nil: // expected error
			err = translate(err, lang) // ******** l10n ********
			t.Log("#"+fmt.Sprintf("%d", i+1), "OK, error expected:", err)
		default:
			t.Log("#"+fmt.Sprintf("%d", i+1), "OK")
		}
	}
}
