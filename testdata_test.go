// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"os"
	"path/filepath"

	"cloudeng.io/errors"
	"cloudeng.io/sys/windows/win32testutil"
)

type testtree struct {
	root         string
	depth        int
	breadth      int
	nfiles       int
	baseContent  []string
	addedContent []string
}

func (tt *testtree) createLevel(parent string, level int) ([]string, error) {
	created := []string{}
	contents := []byte{0x1}
	for i := 0; i < tt.nfiles; i++ {
		filename := filepath.Join(parent, fmt.Sprintf("f%02v-%02v", level, i))
		if err := os.WriteFile(filename, contents, 0666); err != nil { //nolint:gosec
			return nil, err
		}
		contents = append(contents, 0x1)
		created = append(created, filename)
	}

	for i := 0; i < tt.breadth; i++ {
		dirname := filepath.Join(parent, fmt.Sprintf("d%02v-%02v", level, i))
		if err := os.Mkdir(dirname, 0777); err != nil {
			return nil, err
		}
		created = append(created, dirname)
	}

	j := filepath.Join
	errs := errors.M{}
	err := os.Mkdir(j(parent, "d-inaccessible-dir"), 0000)
	errs.Append(err)
	err = win32testutil.MakeInaccessibleToOwner(j(parent, "d-inaccessible-dir"))
	errs.Append(err)
	err = os.Symlink(j("f0"), j(parent, "f-soft-link-f0"))
	errs.Append(err)
	err = os.Symlink("nowhere", j(parent, "f-soft-link-f1"))
	errs.Append(err)
	err = os.WriteFile(j(parent, "f-inaccessible-file"), []byte{'1', '2', '3'}, 0000)
	errs.Append(err)
	err = win32testutil.MakeInaccessibleToOwner(j(parent, "f-inaccessible-file")) // windows.
	errs.Append(err)

	created = append(created,
		j(parent, "d-inaccessible-dir"),
		j(parent, "f-soft-link-f0"),
		j(parent, "f-soft-link-f1"),
		j(parent, "f-inaccessible-file"),
	)
	return created, errs.Err()
}

// createTree creates a tree of files and directories with the specified
// depth and breadth. Each directory will contain nfiles files.
// In additon, inaccessible files and directories are created, as are soft links.
func (tt *testtree) createTree(parent string, level int, created []string) ([]string, error) {
	if tt.depth == level {
		return created, nil
	}
	d, err := tt.createLevel(parent, level)
	if err != nil {
		return nil, err
	}
	created = append(created, d...)
	for i := 0; i < tt.breadth; i++ {
		parent := filepath.Join(parent, fmt.Sprintf("d%02v-%02v", level, i))
		c, err := tt.createTree(parent, level+1, created)
		if err != nil {
			return nil, err
		}
		created = c
	}
	return created, nil
}

func (tt *testtree) createBase() error {
	c, err := tt.createTree(tt.root, 0, []string{tt.root})
	tt.baseContent = c
	return err
}

func (tt *testtree) base() []string {
	return tt.baseContent
}

func (tt *testtree) createAdditional(depth, nfiles, ndirs int) error {
	p := tt.root
	for i := 0; i < depth; i++ {
		p = filepath.Join(p, fmt.Sprintf("d%02v-00", i))
	}
	fileOffset, dirOffset := 0, 0
	if depth < tt.depth {
		fileOffset, dirOffset = tt.nfiles, tt.breadth
	}
	contents := make([]byte, fileOffset)
	for i := fileOffset; i < fileOffset+nfiles; i++ {
		filename := filepath.Join(p, fmt.Sprintf("f%02v-%02v", depth, i))
		if err := os.WriteFile(filename, contents, 0666); err != nil { //nolint:gosec
			return err
		}
		contents = append(contents, 0x1)
		tt.addedContent = append(tt.addedContent, filename)
	}
	for i := dirOffset; i < dirOffset+ndirs; i++ {
		dirname := filepath.Join(p, fmt.Sprintf("d%02v-%02v", depth, i))
		if err := os.Mkdir(dirname, 0777); err != nil {
			return err
		}
		tt.addedContent = append(tt.addedContent, dirname)
	}
	return nil
}

func (tt *testtree) additional() []string {
	return tt.addedContent
}

func numDirsAndFiles(paths []string) (d, f int64) {
	for _, a := range paths {
		switch filepath.Base(a)[0] {
		case 'd':
			d++
		case 'f':
			f++
		}
	}
	return
}

func (tt *testtree) deletionAdditonal() error {
	// remove files first.
	for _, a := range tt.addedContent {
		b := filepath.Base(a)
		if b[0] == 'f' {
			if err := os.Remove(a); err != nil {
				return err
			}
		}
	}
	for _, a := range tt.addedContent {
		if filepath.Base(a)[0] == 'f' {
			continue
		}
		if err := os.Remove(a); err != nil {
			return err
		}
	}
	tt.addedContent = nil
	return nil
}

func newTestTree(parent string, depth, breadth, nfiles int) *testtree {
	return &testtree{
		root:    filepath.Join(parent, "d-testtree"),
		depth:   depth,
		breadth: breadth,
		nfiles:  nfiles,
	}
}
