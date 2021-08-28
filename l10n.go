// Copyright 2021 Hans-Werner Heinzen. All rights reserved.
// Use of this source code is governed by a license
// that can be found in the LICENSE file.

// l10n can localize error messages if l10n.json is prepared.
//
//go:generate l10n -json=l10n.json

package livedb

import (
	"fmt"
)

func translate(in error, lang string) (out error) {
	fnc := "translate"

	if in == nil {
		return nil
	}

	out, err := L10nLocalizeError(in, lang)
	if err != nil {
		return err
	}
	if out != nil {
		return out
	}
	// else: NOTFOUND

	return fmt.Errorf(fnc+":%w", in)
}
