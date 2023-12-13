// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package prefixinfo

type fsOnly struct {
	ino    uint64
	blocks int64
}

type idAndFS struct {
	fsOnly
	uid, gid uint32
}
