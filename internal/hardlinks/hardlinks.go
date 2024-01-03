// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package hardlinks

type perDevice[T comparable] struct {
	dev    uint64
	inodes map[uint64]T
}

type devices[T comparable] []*perDevice[T]

func (d devices[T]) forDevice(dev uint64) (devices[T], *perDevice[T]) {
	for _, pd := range d {
		if pd.dev == dev {
			return d, pd
		}
	}
	pd := &perDevice[T]{
		dev:    dev,
		inodes: map[uint64]T{},
	}
	return append(d, pd), pd
}

// Incremental tracks devices and inodes to determine if a newly
// seen file or directory is a duplicate, i.e. is a hard link to
// an existing filesystem entries. It is incremental and hence cannot
// detect the first entry in a set of hard links.
type Incremental struct {
	devices devices[struct{}]
}

func (i *Incremental) Ref(dev uint64, ino uint64) bool {
	var pd *perDevice[struct{}]
	i.devices, pd = i.devices.forDevice(dev)
	if _, ok := pd.inodes[ino]; ok {
		return true
	}
	pd.inodes[ino] = struct{}{}
	return false
}
