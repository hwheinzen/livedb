// Copyright 2021 Hans-Werner Heinzen. All rights reserved.
// Use of this source code is governed by a license
// that can be found in the LICENSE file.

// Command generatelivetab generates Go code for a livedb table.
//
// It uses descriptive data from a JSON file.
//
// The JSON file must contain:
// ---------------------------
//	Copyright    - year and copyright owner
//	Package      - name of package  the generated code is meant for
//	ErrorType    - type of used extended error type 
//                 (e.g. Err in github.com/hwheinzen/stringl10n/mistake)
//	Name         - name of type that the table represents
//	Acronym      - unique shortname (2 characters minimum, ASCII only)
//	Atts         - list of attributes
//
// The JSON file may contain:
// --------------------------
//	File         - output filename, default is <Acronym>_generated.go
//	DbName       - database table name - if Name contains non-ASCII characters
//
// Attributes must contain:
// ------------------------
//	Name         - attribute/field name
//	CreateClause - attributes create clause
//
// Attributes may contain:
// -----------------------
//	IsNumType    - true -> int
//	DbName       - database field name - if Name contains non-ASCII characters
//	ReadBy       - true -> function 'by<Name>Ts' will be generated
//
// (Livedb tables only use the types int and string;
//  date, time, and timestamp are stored as strings.)
//
// Example:
/*
{  #-comments are allowed
	"Copyright": "2021 Itts Mee"
	,"Package":  "example"
	,"ErrorType":"Err"
	,"Name":     "Mitarbeiter"
	,"Acronym":  "Mi"
	# --------------- comment ------------------------
	,"Atts":     [
		 {
			 "Name":         "Name"
			,"CreateClause": "varchar(50) not null"
			,"ReadBy":			true
		}
		,{
			 "Name":         "Zusatz"
			,"CreateClause": "varchar(50)"
		}
		,{
			 "Name":         "Notiz"
			,"CreateClause": "varchar(500)"
		}
		,{
			"Name":				"UsID"
			,"CreateClause":	"integer"
			,"IsNumType":		true
			,"ReadBy":			true
		}
	]
}*/
package main
