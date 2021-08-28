// Copyright 2015-21 Hans-Werner Heinzen. All rights reserved.
// Use of this source code is governed by a license
// that can be found in the LICENSE file.

// NOT log AND NOT debug
// +build !log,!debug

package livedb

// Log in nolog.go prints nothing.
func Log(v ...interface{}) {}
