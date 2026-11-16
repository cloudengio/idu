// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

//go:build windows

package badgerdb

import "github.com/dgraph-io/badger/v4"

func osOptions(opts badger.Options) badger.Options {
	opts.ReadOnly = false // Read-only mode is not supported on Windows.
	return opts
}
