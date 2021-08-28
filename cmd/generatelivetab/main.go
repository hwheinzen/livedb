// Copyright 2021 Hans-Werner Heinzen. All rights reserved.
// Use of this source code is governed by a license
// that can be found in the LICENSE file.

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"text/template"
	"time"

	"github.com/dullgiulio/jsoncomments"

	. "github.com/hwheinzen/stringl10n/mistake"
)

const (
	pgm = "generatelivetab"
)

type Values struct {
	// ------------- from JSON file
	File      string
	Copyright string
	Package   string
	ErrorType string
	// ---
	Name    string
	Acronym string // ASCII only
	DbName  string // ASCII only
	Atts    []Att
	// ------------- computed values
	Generator string
	Generated string
	Input     string
	UCPackage string
	LcAcronym string
	UcAcronym string
	UCAcronym string
	LcName    string
	// ---
	TypeTemplate string
	NameTemplate string
	Nam2Template string
	IDTemplate   string
	ValTemplate  string
	Val2Template string
	IntTemplate  string
	Int2Template string
}
type Att struct {
	// ------------- from JSON file
	Name          string
	Type          string
	IsNumType     bool
	IsForeignType bool
	DbName        string // ASCII only
	CreateClause  string // ASCII only
	ReadBy        bool
	// ---
	LcName        string
}

// buildtime serves 'l10n -version' if l10n was built with:
// -ldflags "-X 'main.buildtime=`date -Iseconds`'"
var buildtime string

func main() {
	fnc := "main"

	jsonFile, lang := args(buildtime)

	vals, err := getValues(jsonFile)
	if err != nil {
		err = translate(err, lang) // ******** l10n ********
		log.Fatalln(pgm+":"+fnc+":"+err.Error())
	}

	err = makeCode(&vals)
	if err != nil {
		err = translate(err, lang) // ******** l10n ********
		log.Fatalln(pgm+":"+fnc+":"+err.Error())
	}
	err = gofmt(vals.File)
	if err != nil {
		err = translate(err, lang) // ******** l10n ********
		log.Fatalln(pgm+":"+fnc+":"+err.Error())
	}
}

func gofmt(fn string) error {
	fnc := "gofmt"

	cmd := exec.Command("gofmt", "-w", fn)
	stdOutErr, err := cmd.CombinedOutput()
	if err != nil {
	   return fmt.Errorf(fnc+":"+string(stdOutErr)+"\n"+fnc+":%w", err)
	}

	return nil
}

func getValues(jsonFile string) (vals Values, err error) {
	fnc := "getValues"

	file, err := os.Open(jsonFile)
	if err != nil {
		e := Err{
			Fix: "GENERATELIVETAB:open {{.Name}} failed",
			Var: []struct {Name  string; Value interface{}}{
				{"Name", jsonFile},
			},
		}
		return vals, fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}
	defer file.Close()

	reader := jsoncomments.NewReader(file) // filters #-comments
	dec := json.NewDecoder(reader)

	err = dec.Decode(&vals)
	if err != nil {
		e := Err{
			Fix: "GENERATELIVETAB:decode JSON from {{.Name}} failed",
			Var: []struct {Name  string; Value interface{}}{
				{"Name", jsonFile},
			},
		}
		return vals, fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	if vals.Copyright == "" {
		err := Err{
			Fix: "GENERATELIVETAB:{{.Nam2}} in {{.Name}} is missing",
			Var: []struct {Name  string; Value interface{}}{
				{"Name", jsonFile},
				{"Nam2", "Copyright"},
			},
		}
		return vals, fmt.Errorf(fnc+":%w", err)
	}
	if vals.Package == "" {
		err := Err{
			Fix: "GENERATELIVETAB:{{.Nam2}} in {{.Name}} is missing",
			Var: []struct {Name  string; Value interface{}}{
				{"Name", jsonFile},
				{"Nam2", "Package"},
			},
		}
		return vals, fmt.Errorf(fnc+":%w", err)
	}
	if vals.ErrorType == "" {
		err := Err{
			Fix: "GENERATELIVETAB:{{.Nam2}} in {{.Name}} is missing",
			Var: []struct {Name  string; Value interface{}}{
				{"Name", jsonFile},
				{"Nam2", "ErrorType"},
			},
		}
		return vals, fmt.Errorf(fnc+":%w", err)
	}

	if vals.Name == "" {
		err := Err{
			Fix: "GENERATELIVETAB:{{.Nam2}} in {{.Name}} is missing",
			Var: []struct {Name  string; Value interface{}}{
				{"Name", jsonFile},
				{"Nam2", "Name"},
			},
		}
		return vals, fmt.Errorf(fnc+":%w", err)
	}
	if len(vals.Acronym) < 2 {
		err := Err{
			Fix: "GENERATELIVETAB:{{.Nam2}} in {{.Name}} too short",
			Var: []struct {Name  string; Value interface{}}{
				{"Name", jsonFile},
				{"Nam2", "Acronym"},
			},
		}
		return vals, fmt.Errorf(fnc+":%w", err)
	}

	for i, v := range vals.Atts {
		if v.Name == "" {
			err := Err{
				Fix: "GENERATELIVETAB:{{.Nam2}} in {{.Name}} is missing",
				Var: []struct {Name  string; Value interface{}}{
					{"Name", jsonFile},
					{"Nam2", "Atts.Name"},
				},
			}
			return vals, fmt.Errorf(fnc+":%w", err)
		}
		vals.Atts[i].LcName = strings.ToLower(v.Name)

		if v.CreateClause == "" {
			err := Err{
				Fix: "GENERATELIVETAB:{{.Nam2}} in {{.Name}} is missing",
				Var: []struct {Name  string; Value interface{}}{
					{"Name", jsonFile},
					{"Nam2", "Atts.CreateClause"},
				},
			}
			return vals, fmt.Errorf(fnc+":%w", err)
		}
	}

	// populate rest of vals

	vals.Generator = pgm
	vals.Generated = time.Now().String()[:40]
	vals.Input = jsonFile

	vals.UCPackage = strings.ToUpper(vals.Package)

	vals.LcAcronym = strings.ToLower(vals.Acronym)
	vals.UcAcronym = strings.ToUpper(vals.Acronym[0:1]) + vals.Acronym[1:]
	vals.UCAcronym = strings.ToUpper(vals.Acronym)

	vals.LcName = strings.ToLower(vals.Name)

	if vals.File == "" {
		vals.File = vals.LcAcronym + "_generated.go"
	}

	if vals.DbName == "" {
		vals.DbName = "t" + vals.LcName
	}

	for i, v := range vals.Atts {
		if v.DbName == "" {
			vals.Atts[i].DbName = strings.ToLower(v.Name)
		}
 	}

	vals.TypeTemplate = "{{.Type}}"
	vals.NameTemplate = "{{.Name}}"
	vals.Nam2Template = "{{.Nam2}}"
	vals.ValTemplate = "{{.Val}}"
	vals.Val2Template = "{{.Val2}}"
	vals.IDTemplate = "{{.Int1}}"
	vals.IntTemplate = "{{.Int}}"
	vals.Int2Template = "{{.Int2}}"

	return vals, nil
}

func makeCode(vals *Values) error {
	fnc := "makeCode"

	out, err := os.Create(vals.File)
	if err != nil {
		e := Err{
			Fix: "GENERATELIVETAB:create file {{.Name}} failed",
			Var: []struct {Name  string; Value interface{}}{
				{"Name", vals.File},
			},
		}
		return fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}
	defer out.Close()

	t := template.New("tmpl")
	_, err = t.Parse(tmpl)
	if err != nil {
		e := Err{
			Fix: "GENERATELIVETAB:parse template {{.Name}} failed",
			Var: []struct {Name  string; Value interface{}}{
				{"Name", "tmpl"},
			},
		}
		return fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}

	err = t.Execute(out, vals)
	if err != nil {
		e := Err{
			Fix: "GENERATELIVETAB:execute template {{.Name}} failed",
			Var: []struct {Name  string; Value interface{}}{
				{"Name", "tmpl"},
			},
		}
		return fmt.Errorf(fnc+":%w:"+err.Error(), e)
	}
	
	return nil
}
