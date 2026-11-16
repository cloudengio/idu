// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

//go:build !unix && !windows

package iduconfig

import "errors"

// blocksReportAllocation is false since there is no portable means of
// determining the space allocated to a file on these systems.
const blocksReportAllocation = false

func filesystemBlockSize(string) (int64, error) {
	return 0, errors.ErrUnsupported
}
