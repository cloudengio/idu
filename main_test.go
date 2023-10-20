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

func TestMain(m *testing.M) {
	tmpDir, _ := os.MkdirTemp("", "idu")
	bin, err := executil.GoBuild(context.Background(), filepath.Join(tmpDir, "idu"), "cloudeng.io/cmd/idu")
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
		"analyze disk usage using a database for incremental updates",
		"errors - list the errors stored in the database",
		"config - describe the current configuration",
	}
	if err := containsAnyOf(out, base...); err != nil {
		t.Fatal(err)
	}
	out, _ = runIDU("--help") // will return exit status 1 for --help
	if err := containsAnyOf(out, base...); err != nil {
		t.Fatal(err)
	}
	err := containsAnyOf(out, "[--config=$HOME/.idu.yml --gcpercent=50 --http= --log-dir=. --profile= --stderr=false --units=decimal --v=0]")
	if err != nil {
		t.Fatal(err)
	}
	out, _ = runIDU("help", "analyze") // will return exit status 1 for help.

	err = containsAnyOf(out, "Usage of command analyze: analyze the file system to build a database of file counts, disk usage etc",
		"analyze [--newer=24h0m0s --use-db=false] <prefix>")
	if err != nil {
		t.Fatal(err)
	}
}
