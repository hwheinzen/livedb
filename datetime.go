// Copyright 2010-21 Hans-Werner Heinzen. All rights reserved.
// Use of this source code is governed by a license
// that can be found in the LICENSE file.

package livedb

import (
	"database/sql"
	"fmt"

	. "github.com/hwheinzen/stringl10n/mistake"
)

// IsTmsp returns true if the given string conforms to the timestamp
// format 'YYYY-MM-DD HH:MM:SS.sss' and if it is valid.
func IsTmsp(tmsp string, tx *sql.Tx) (bool, error) {
	fnc := "IsTmsp"

	s := "select " + FormatTmsp(1) + ";"

	Log("s:", s)
	Log("tmsp:", tmsp)

	rows := &sql.Rows{}
	var err error

	if tx != nil {
		rows, err = tx.Query(s, tmsp)
	} else {
		rows, err = GDb.Query(s, tmsp)
	}
	if err != nil {
		return false, nil // we asume postgres reported invalid date
		// 		e := Err{Fix: "LIVEDB:error executing query"}
		// 		return false, fmt.Errorf(fnc+":%w:"+err.Error(), e)

	}
	defer rows.Close()

	if !rows.Next() {
		err = rows.Err()
		if err != nil {
			e := Err{Fix: "LIVEDB:error nexting query"}
			return false, fmt.Errorf(fnc+":%w:"+err.Error(), e)
		}
		err = Err{Fix: "LIVEDB:error nexting query"}
		return false, fmt.Errorf(fnc+":%w", err)
	}

	nS1 := sql.NullString{}
	err = rows.Scan(&nS1)
	if err != nil {
		e := Err{Fix: "LIVEDB:error scanning row"}
		return false, fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	if !nS1.Valid { // WORKS with sqlite
		return false, nil
	}

	return true, nil
}

// CurrentTmsp returns the current timestamp at timezone UTC
// as string formatted as 'YYYY-MM-DD HH:MM:SS.sss'.
func CurrentTmsp(tx *sql.Tx) (string, error) {
	fnc := "CurrentTmsp"

	s := "select " + FormatNow() + ";"

	Log("s:", s)

	rows := &sql.Rows{}
	var err error

	if tx != nil {
		rows, err = tx.Query(s)
	} else {
		rows, err = GDb.Query(s)
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
		return "", fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}
	defer rows.Close()

	if !rows.Next() {
		err = rows.Err()
		if err != nil {
			e := Err{Fix: "LIVEDB:error nexting query"}
			return "", fmt.Errorf(fnc+":%w:"+err.Error(), e)
		}
		err = Err{Fix: "LIVEDB:error nexting query"}
		return "", fmt.Errorf(fnc+":%w", err)
	}

	var ts string
	err = rows.Scan(&ts)
	if err != nil {
		e := Err{Fix: "LIVEDB:error scanning row"}
		return "", fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	return ts, nil
}

// CmpTmspRef let the database check if tmsp is past, present or future
// of a reference timestamp.
//
// NOTE: The compare stops at seconds.
func CmpTmspRef(tmsp, ref string, tx *sql.Tx) (past, present, future bool, err error) {
	fnc := "CmpTmspRef"

	if len(ref) < 19 {
		err := Err{
			Fix: "LIVEDB:timestamp {{.Name}} too short: {{.Tmsp}}, expected {{.Int}}+ characters",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{Name: "Name", Value: "ref"},
				{Name: "Tmsp", Value: ref},
				{Name: "Int", Value: 19},
			},
		}
		return false, false, false, fmt.Errorf(fnc+":%w", err)
	}
	if len(tmsp) < 19 {
		err := Err{
			Fix: "LIVEDB:timestamp {{.Name}} too short: {{.Tmsp}}, expected {{.Int}}+ characters",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{Name: "Name", Value: "tmsp"},
				{Name: "Tmsp", Value: tmsp},
				{Name: "Int", Value: 19},
			},
		}
		return false, false, false, fmt.Errorf(fnc+":%w", err)
	}

	s := "select " + FormatDiffTmsp(&ref, &tmsp) + ";"

	Log("s:", s)
	Log("sqlargs:", ref[:19], tmsp[:19])

	rows := &sql.Rows{}

	if tx != nil {
		rows, err = tx.Query(s, ref[:19], tmsp[:19])
	} else {
		rows, err = GDb.Query(s, ref[:19], tmsp[:19])
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
		return false, false, false, fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}
	defer rows.Close()

	if !rows.Next() {
		err = rows.Err()
		if err != nil {
			e := Err{Fix: "LIVEDB:error nexting query"}
			return false, false, false, fmt.Errorf(fnc+":%w:"+err.Error(), e)
		}
		err = Err{Fix: "LIVEDB:error nexting query"}
		return false, false, false, fmt.Errorf(fnc+":%w", err)
	}

	var n float64
	err = rows.Scan(&n)
	if err != nil {
		e := Err{Fix: "LIVEDB:error scanning row"}
		return false, false, false, fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}
	if n > 0 {
		past = true
	} else if n == 0 {
		present = true
	} else {
		future = true
	}

	return past, present, future, nil
}

// CmpTmspNow let the database check if tmsp is past, present or future.
//
// NOTE: The compare stops at seconds.
func CmpTmspNow(tmsp string, tx *sql.Tx) (past, present, future bool, err error) {
	fnc := "CmpTmspNow"

	if len(tmsp) < 19 {
		err := Err{
			Fix: "LIVEDB:timestamp {{.Name}} too short: {{.Tmsp}}, expected {{.Int}}+ characters",
			Var: []struct {
				Name  string
				Value interface{}
			}{
				{Name: "Name", Value: "tmsp"},
				{Name: "Tmsp", Value: tmsp},
				{Name: "Int", Value: 19},
			},
		}
		return false, false, false, fmt.Errorf(fnc+":%w", err)
	}

	s := "select " + FormatDiffNow() + ";"

	Log("s:", s)
	Log("sqlargs:", tmsp[:19])

	rows := &sql.Rows{}

	if tx != nil {
		rows, err = tx.Query(s, tmsp[:19])
	} else {
		rows, err = GDb.Query(s, tmsp[:19])
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
		return false, false, false, fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}
	defer rows.Close()

	if !rows.Next() {
		err = rows.Err()
		if err != nil {
			e := Err{Fix: "LIVEDB:error nexting query"}
			return false, false, false, fmt.Errorf(fnc+":%w:"+err.Error(), e)
		}
		err = Err{Fix: "LIVEDB:error nexting query"}
		return false, false, false, fmt.Errorf(fnc+":%w", err)
	}

	var n float64
	err = rows.Scan(&n)
	if err != nil {
		e := Err{Fix: "LIVEDB:error scanning row"}
		return false, false, false, fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	if n > 0 {
		past = true
	} else if n == 0 {
		present = true
	} else {
		future = true
	}

	return past, present, future, nil
}

// Tmsp checks if the given string is a valid timestamp
// ("now" is also valid).
//
// NOTE: The compare stops at seconds.
func Tmsp(in string, tx *sql.Tx) (out string, past, present, future bool, err error) {
	fnc := "Tmsp"

	if in == Now {

		out, err = CurrentTmsp(tx)
		if err != nil {
			return "", false, false, false, fmt.Errorf(fnc+":%w", err)
		}
		present = true

	} else {

		var ok bool
		ok, err = IsTmsp(in, tx)
		if err != nil {
			return "", false, false, false, fmt.Errorf(fnc+":%w", err)
		}
		if !ok {
			err = Err{Fix: "LIVEDB:not a valid timestamp"}
			return "", false, false, false, fmt.Errorf(fnc+":%w", err)
		}

		past, present, future, err = CmpTmspNow(in, tx)
		if err != nil {
			return "", false, false, false, fmt.Errorf(fnc+":%w", err)
		}
		out = in
	}

	return
}
