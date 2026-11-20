// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package clitest

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"cloudeng.io/errors"
	"cloudeng.io/windows/win32testutil"
)

type TestTree struct {
	Root         string
	depth        int
	breadth      int
	nfiles       int
	baseContent  []string
	addedContent []string
}

func (tt *TestTree) createLevel(parent string, level int) ([]string, error) {
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
func (tt *TestTree) createTree(parent string, level int, created []string) ([]string, error) {
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

func (tt *TestTree) CreateBase() error {
	c, err := tt.createTree(tt.Root, 0, []string{tt.Root})
	tt.baseContent = c
	return err
}

func (tt *TestTree) Base() []string {
	return tt.baseContent
}

func (tt *TestTree) CreateAdditional(depth, nfiles, ndirs int) error {
	p := tt.Root
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

func (tt *TestTree) Additional() []string {
	return tt.addedContent
}

func NumDirsAndFiles(paths []string) (d, f int64) {
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

func (tt *TestTree) DeleteAdditional() error {
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

func NewTestTree(parent string, depth, breadth, nfiles int) *TestTree {
	return &TestTree{
		Root:    filepath.Join(parent, "d-testtree"),
		depth:   depth,
		breadth: breadth,
		nfiles:  nfiles,
	}
}

// SetupAnalyze creates a TestTree, with its base content, and a
// configuration file for it, named testtree, that excludes part of it.
// It returns the temporary directory containing them, the configuration
// file and the root of the tree.
func SetupAnalyze(t *testing.T) (tmpDir, config, prefix string, tt *TestTree) {
	tmpDir, err := os.MkdirTemp("", "filewalk")
	if err != nil {
		t.Fatalf("failed to create testdir: %v", err)
	}

	tt = NewTestTree(tmpDir, 3, 5, 5)

	testTree, dbDir := tt.Root, filepath.Join(tmpDir, "database")
	for _, d := range []string{testTree, dbDir} {
		if err := os.Mkdir(d, 0777); err != nil {
			t.Fatal(err)
		}
	}

	if err := tt.CreateBase(); err != nil {
		t.Fatal(err)
	}

	cfg := fmt.Sprintf(`filesystems:
  - name: testtree
    prefix: %v
    database: %v/db
    concurrent_scans: 2
    concurrent_stats: 4
    scan_size: 2
    exclusions:
      - 'd-testtree[/\\]d00-01'
`, testTree, dbDir)
	err = os.WriteFile(filepath.Join(tmpDir, "config.yaml"), []byte(cfg), 0600)
	if err != nil {
		t.Fatal(err)
	}
	return tmpDir, filepath.Join(tmpDir, "config.yaml"), testTree, tt
}

// CaptureStdout returns the output written to os.Stdout by fn.
func CaptureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	defer func() { os.Stdout = orig }()

	fnErr := fn()
	_ = w.Close()
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	_ = r.Close()
	return buf.String(), fnErr
}
