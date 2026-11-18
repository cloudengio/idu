// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import "strings"

// SubtreeMatch reports whether key equals target or is beneath target
// delimited by sep, and whether lexicographical scan has passed target's
// subtree. Sibling keys sorting between target and target+sep (such as /a-b
// for /a) return (false, false) so scanning skips them and continues to
// /a/... descendants.
func SubtreeMatch(key, target, sep string) (match, stop bool) {
	if key == target {
		return true, false
	}
	if strings.HasSuffix(target, sep) {
		if strings.HasPrefix(key, target) {
			return true, false
		}
		return false, true
	}
	childPrefix := target + sep
	if strings.HasPrefix(key, childPrefix) {
		return true, false
	}
	if !strings.HasPrefix(key, target) || key > childPrefix {
		return false, true
	}
	return false, false
}
