// Copyright 2021 Hans-Werner Heinzen. All rights reserved.
// Use of this source code is governed by a license
// that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"os"

	. "github.com/hwheinzen/stringl10n/mistake"
)

// args reads flags and arguments and returns the JSON file name.
//
// -json		<name of JSON file> (MUST)
// -lang			<default: en>
// -version
//  -help
//
// Flag -help only prints a usage description.
func args(buildtime string) (jsonFile, lang string) {
//	fnc := "args"

	var version bool
	flag.BoolVar(&version, "version", false, "(if built with -ldflags \"-X main.buildtime '```date`'\"") // ``` seem to be necessary for PrintDefaults()

	var help bool
	flag.BoolVar(&help, "help", false, "usage")

	flag.StringVar(&jsonFile, "json", "", "file name (MUST)")

	flag.StringVar(&lang, "lang", "en", "language of error messages")

	flag.Parse()

	if help {
		flag.Usage()
		os.Exit(0)
	}

	if version {
		if buildtime == "" {
			inf := Err{
				Fix: "GENERATELIVETAB:{{.Name}}:unknown version",
				Var: []struct {Name  string; Value interface{}}{
					{"Name", pgm},
				},
			}
			fmt.Println(translate(inf, lang))
		} else {
			inf := Err{
				Fix: "GENERATELIVETAB:{{.Name}}:version of {{.Nam2}}",
				Var: []struct {Name  string; Value interface{}}{
					{"Name", pgm},
					{"Nam2", buildtime},
				},
			}
			fmt.Println(translate(inf, lang))
		}
		os.Exit(0)
	}

	if jsonFile == "" {
		err := Err{
			Fix: "GENERATELIVETAB:{{.Name}}:{{.Nam2}} argument missing",
			Var: []struct {Name  string; Value interface{}}{
				{"Name", pgm},
				{"Nam2", "-json"},
			},
		}
		fmt.Fprintln(os.Stderr, translate(err, lang))
		flag.Usage()
		os.Exit(2)
	}

	return jsonFile, lang
}
