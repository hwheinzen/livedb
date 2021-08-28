// Copyright 2010 Hans-Werner Heinzen. All rights reserved.
// Use of this source code is governed by a license
// that can be found in the LICENSE file.

// SQL syntax differs slightly for different databases.
// Here are the specifics for SQLite.
//
// NOT mysql AND NOT postgresql
// +build !mysql,!postgresql

package livedb

import "os/exec"

const gDbOpen = "testdb"

func removeDb() (err error) { // it's simple for sqlite
	cmd := exec.Command("/bin/rm", gDbOpen)
	err = cmd.Run()
	return
}

var isDateTests = []isDateTest{
	{"2", true},                             // false but sqlite says true
	{"20", true},                            // false but sqlite says true
	{"200", true},                           // false but sqlite says true
	{"2000", true},                          // false but sqlite says true
	{"2000-05", false},                      //
	{"1900-01-01", true},                    //
	{"9999-12-31", true},                    //
	{"w010-01-01", false},                   //
	{"2010-02-29", true},                    // false but sqlite says true
	{"2010-04-31", true},                    // false but sqlite says true
	{"2010-08-11", true},                    //
	{"99999-01-01", false},                  // true but sqlite says false
	{"2015-01-01 11:33", true},              //
	{"2015-01-01 11:33:44", true},           //
	{"2015-01-01 11:33:44.987", true},       //
	{"2015-01-01 11:33:44.987654", true},    //
	{"2015-01-01 11:33:44.987654321", true}, //
	{"2015-01-01 24:33", true},              // false but sqlite says true
	{"2015-01-01 11:33:61", false},          //
}

var startTests = []startTest{
	{Now, "", "0000String", 0, true},                                   // OK Now
	{Now, "0815String", "4711String", 42, true},                        // OK Now
	{"2999-01-01 00:00:00.000", "0816String", "4712String", 43, true},  // OK fut
	{"1900-01-01 12:00:00.000", "0817String", "4713String", 44, false}, // past
	{"xyz9-01-01 00:00:00.000", "0818String", "4714String", 45, false}, // wrong
}

var changeTests = []changeTest{
	{Now, "0815String", 42, Now, "4711String", 42, true},                                               // OK
	{"2100-01-01 00:00:00.000", "0816String", 43, "2100-11-11 00:00:00.000", "0816String", 4711, true}, // OK
	{Now, "0817String", 44, Now, "0817String", 44, true},                                               // OK. no change
	{"2101-01-01 00:00:00.000", "0818String", 45, "2101-01-01 00:00:00.000", "0818String", 45, true},   // OK, no change
}

var terminateTests = []terminateTest{
	{"2100-01-01 00:00:00.000", "0815String", 42, 2, 100, "2100-06-06 00:00:00.000", true}, // OK ==> 1 record
	{"2100-01-01 00:00:00.000", "0816String", 43, 5, 100, "2250-06-06 00:00:00.000", true}, // OK ==> 2 records
	{"2100-01-01 00:00:00.000", "0817String", 44, 0, 100, "2100-01-01 00:00:00.000", true}, // OK ==> no record
	{"2100-01-01 00:00:00.000", "0818String", 45, 2, 100, "2100-01-01 00:00:00.000", true}, // OK ==> no record
	{"2100-01-01 00:00:00.000", "0819String", 46, 2, 100, "2200-01-01 00:00:00.000", true}, // OK ==> 1 record
}

var moveUntilTests = []moveUntilTest{
	{"2100-01-01 00:00:00.000", "0815String", 42, 2, 100, "2100-06-06 00:00:00.000", true}, // OK ==> 3 record
	{"2100-01-01 00:00:00.000", "0816String", 43, 5, 100, "2250-06-06 00:00:00.000", true}, // OK ==> 6 records
	{"2100-01-01 00:00:00.000", "0817String", 44, 0, 100, "2100-01-01 00:00:00.000", true}, // OK ==> no record
	{"2100-01-01 00:00:00.000", "0818String", 45, 2, 100, "2100-01-01 00:00:00.000", true}, // OK ==> 2 records
	{"2100-01-01 00:00:00.000", "0819String", 46, 2, 100, "2200-01-01 00:00:00.000", true}, // OK ==> 2 record
}

var moveBeginTests = []moveBeginTest{
	{"2100-01-01 00:00:00.000", "0815String", 42, 2, 100, "2100-06-06 00:00:00.000", true}, // OK ==> 3 record
	{"2100-01-01 00:00:00.000", "0816String", 43, 5, 100, "2250-06-06 00:00:00.000", true}, // OK ==> 6 records
	{"2100-01-01 00:00:00.000", "0817String", 44, 0, 100, "2100-01-01 00:00:00.000", true}, // OK ==> 1 record
	{"2100-01-01 00:00:00.000", "0818String", 45, 2, 100, "2100-01-01 00:00:00.000", true}, // OK ==> 3 records
	{"2100-01-01 00:00:00.000", "0819String", 46, 2, 100, "2200-01-01 00:00:00.000", true}, // OK ==> 3 record
}
