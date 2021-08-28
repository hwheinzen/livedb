// Copyright 2010-21 Hans-Werner Heinzen. All rights reserved.
// Use of this source code is governed by a license
// that can be found in the LICENSE file.

// Tests for the date/time functions.

package livedb

import (
	"fmt"
	"testing"
)

// TestCurrentTmsp retrieves the current timestamp from the
// database and then tests the result.
func TestCurrentTmsp(t *testing.T) {

	ts, err := CurrentTmsp(nil)
	if err != nil {
		err = translate(err, lang) // ******** l10n ********
		t.Error(err)
	}

	t.Log("current timestamp:", ts)

	ok, err := IsTmsp(ts, nil)
	if err != nil {
		err = translate(err, lang) // ******** l10n ********
		t.Error(err)
	}
	if !ok {
		t.Error("not a valid timestamp:", ts)
	}
}

type isDateTest struct {
	in string
	ok bool
}

// isDateTests is in livedb_sqlite|postgresql|mysql_test.go

// TestIsTmsp passes if all the test cases return the expected results.
func TestIsTmsp(t *testing.T) {

	for i, v := range isDateTests {
		ok, err := IsTmsp(v.in, nil)
		if err != nil {
			err = translate(err, lang) // ******** l10n ********
			t.Error(err)
		}
		switch {
		case !v.ok && ok:
			t.Error("#"+fmt.Sprintf("%d", i+1), "expected error, got ok")
		case v.ok && !ok:
			t.Errorf("#"+fmt.Sprintf("%d", i+1)+" expected ok, but %s is not valid", v.in)
		case !ok: // expected error
			t.Logf("#"+fmt.Sprintf("%d", i+1)+" OK, error expected: %s is not valid", v.in)
		default:
			t.Logf("#"+fmt.Sprintf("%d", i+1)+" OK: %s", v.in)
		}
	}
}

// TestTmspErr passes when all defined error situations
// make Tmsp() return error.
func TestTmspErr(t *testing.T) {

	type tmspErrTest struct {
		in string
		ok bool
	}
	tmspErrTests := []tmspErrTest{
		{"", false},
	}

	for i, v := range tmspErrTests {
		_, _, _, _, err := Tmsp(v.in, nil)
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
			t.Logf("#"+fmt.Sprintf("%d", i+1)+" OK: %s", v.in)
		}
	}
}

// TestTmsp passes when all defined error situations
// make Tmsp() return error.
func TestTmsp(t *testing.T) {

	type tmspTest struct {
		in            string
		pst, now, fut bool // past, now, future
	}
	tmspTests := []tmspTest{
		{"1900-01-01 12:00:00.000000", true, false, false},
		{Now, false, true, false},
		{"2999-12-31 12:00:00.000000", false, false, true},
	}

	for i, v := range tmspTests {
		out, pst, now, fut, err := Tmsp(v.in, nil)
		if err != nil {
			err = translate(err, lang) // ******** l10n ********
			t.Error(err)
		}
		if pst != v.pst || now != v.now || fut != v.fut {
			t.Errorf("#"+fmt.Sprintf("%d", i+1)+" Tmsp(%s) = %s %v, %v, %v, expected:  %v, %v, %v", v.in, out, pst, now, fut, v.pst, v.now, v.fut)
		}
	}
}

// TestVglNow passes if all the test cases return the expected results.
func TestCmpTmspNow(t *testing.T) {

	type vglNowTest struct {
		in                    string
		past, present, future bool
	}
	vglNowTests := []vglNowTest{
		{"1900-01-01 12:00:00.000000", true, false, false},
		{"2999-12-31 00:00:00.000000", false, false, true},
	}

	for i, v := range vglNowTests {
		_, past, present, future, err := Tmsp(v.in, nil)
		if err != nil {
			err = translate(err, lang) // ******** l10n ********
			t.Error(err)
		}
		if past != v.past || present != v.present || future != v.future {
			t.Errorf("#"+fmt.Sprintf("%d", i+1)+"Tmsp(%s) = %v, %v, %v, expected:  %v, %v, %v", v.in, past, present, future, v.past, v.present, v.future)
		}
	}
}

// TestVglTmsps passes if all the test cases return the expected results.
func TestCmpTmspRef(t *testing.T) {
	fncname := "TestVglTmsps"

	type vglTmspsTest struct {
		ref, ts               string
		past, present, future bool
	}
	vglTmspsTests := []vglTmspsTest{
		{"1900-01-01 12:00:01", "1900-01-01 12:00:00", true, false, false},
		{"2999-12-31 12:00:00", "2999-12-31 12:00:00", false, true, false},
		{"2999-12-31 12:00:00", "2999-12-31 12:00:01", false, false, true},
	}

	for i, v := range vglTmspsTests {
		past, present, future, err := CmpTmspRef(v.ts, v.ref, nil)
		if err != nil {
			t.Error(fncname+":", err)
		}
		if past != v.past || present != v.present || future != v.future {
			t.Errorf("#"+fmt.Sprintf("%d", i+1)+" vglTmsps(%s, %s) = %v, %v, %v, expected:  %v, %v, %v", v.ref, v.ts, past, present, future, v.past, v.present, v.future)
		}
	}
}
