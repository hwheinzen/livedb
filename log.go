// Copyright 2015-21 Hans-Werner Heinzen. All rights reserved.
// Use of this source code is governed by a license
// that can be found in the LICENSE file.

// log OR debug
// +build log debug

package livedb

import "log"

// Log in log.go prints to log.
func Log(v ...interface{}) {
	log.Println(v...)
}
