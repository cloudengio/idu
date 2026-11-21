// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

//go:build windows

package badgerdb

import "github.com/dgraph-io/badger/v4"

func osOptions(opts badger.Options) badger.Options {
	if opts.ReadOnly {
		// Badger's Windows directory lock rejects ReadOnly; applock already
		// coordinates readers and writers.
		opts.BypassLockGuard = true
	}
	return opts
}
