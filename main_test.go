// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"cloudeng.io/os/executil"
)

var iduCommand string

func buildIDU(tmpDir string) (string, error) {
	bin, err := executil.GoBuild(context.Background(), filepath.Join(tmpDir, "idu"), "cloudeng.io/cmd/idu")
	if err != nil {
		return "", err
	}
	fmt.Printf("BIN: %v\n", bin)
	return bin, nil
}

func TestMain(m *testing.M) {
	tmpDir, _ := os.MkdirTemp("", "idu")
	bin, err := buildIDU(tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		os.Exit(1)
	}
	iduCommand = bin
	code := m.Run()
	os.RemoveAll(tmpDir)
	os.Exit(code)
}

func runIDU(args ...string) (string, error) {
	cmd := exec.Command(iduCommand, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("%v: %v", strings.Join(cmd.Args, " "), err)
	}
	return string(out), nil
}

func containsAnyOf(got string, expected ...string) error {
	for _, want := range expected {
		if !strings.Contains(got, want) {
			_, file, line, _ := runtime.Caller(1)
			return fmt.Errorf("%v:%v: %q does not contain %v", filepath.Base(file), line, got, want)
		}
	}
	return nil
}

func TestHelp(t *testing.T) {
	out, _ := runIDU("help") // will return exit status 1 for help.
	base := []string{
		"idu: analyze file systems to create a database of per-file and aggregate size",
		"errors - list the contents of the errors database",
		"flag: help requested",
	}
	if err := containsAnyOf(out, base...); err != nil {
		t.Fatal(err)
	}
	out, _ = runIDU("--help") // will return exit status 1 for --help
	if err := containsAnyOf(out, base...); err != nil {
		t.Fatal(err)
	}
	err := containsAnyOf(out, "[--config=$HOME/.idu.yml --exit-profile= --gcpercent=50 --h=true --http= --units=decimal --v=0]")
	if err != nil {
		t.Fatal(err)
	}
	out, _ = runIDU("help", "analyze") // will return exit status 1 for help.

	err = containsAnyOf(out, "Usage of command analyze: analyze the file system to build a database of file counts, disk usage etc",
		" -incremental")
	if err != nil {
		t.Fatal(err)
	}
}
