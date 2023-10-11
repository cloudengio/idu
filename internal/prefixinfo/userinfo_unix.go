// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package prefixinfo

import (
	"syscall"

	"cloudeng.io/file"
)

func userGroupID(fi file.Info) (userID, groupID uint32, ok bool) {
	if u, ok := fi.Sys().(*syscall.Stat_t); ok {
		return u.Uid, u.Gid, true
	}
	return 0, 0, false
}
