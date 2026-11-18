// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"io/fs"

	"cloudeng.io/cmd/idu/internal/prefixinfo"
)

type prefixInfo struct {
	name string
	prefixinfo.T
}

// PrefixInfoAsFSInfo returns a fs.FileInfo for the supplied PrefixInfo.
func PrefixInfoAsFSInfo(pi prefixinfo.T, name string) fs.FileInfo {
	return &prefixInfo{T: pi, name: name}
}

func (pi *prefixInfo) Name() string {
	return pi.name
}

func (pi *prefixInfo) IsDir() bool {
	return pi.Mode().IsDir()
}

func (pi *prefixInfo) Sys() any {
	return nil
}
