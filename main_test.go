// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

var iduCommand string

func buildIDU(tmpDir string) (string, error) {
	bin := filepath.Join(tmpDir, "idu")
	cmd := exec.Command("go", "build", "-o", bin, "cloudeng.io/cmd/idu")
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "build failed: %v\n%s\n", strings.Join(cmd.Args, " "), out)
		return "", err
	}
	return bin, nil
}

func TestMain(m *testing.M) {
	tmpDir, _ := ioutil.TempDir("", "idu")
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
	dir := filepath.Dir(iduCommand)
	fmt.Printf("DIR: %v: cmd %v\n", dir, iduCommand)
	fi, err := os.Stat(dir)
	if err != nil {
		fmt.Printf("STAT: %v\n", fi.Name())
	} else {
		fmt.Printf("ERR: %v\n", err)
		entries, err := os.ReadDir(dir)
		if err != nil {
			fmt.Printf("ERR: %v\n", err)
		} else {
			for _, e := range entries {
				fmt.Printf("ENTRY: %v\n", e.Name())
			}
		}
	}

	fi, err = os.Stat(iduCommand)
	if err != nil {
		fmt.Printf("STAT CMD: %v\n", fi.Name())
	} else {
		fmt.Printf("ERR CMD: %v\n", err)
	}

	cmd := exec.Command(iduCommand, args...)
	fmt.Printf("CMD: %#v\n", cmd)
	out, err := cmd.CombinedOutput()
	return string(out), fmt.Errorf("%v: %v", strings.Join(cmd.Args, " "), err)
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
	out, err := runIDU("help")
	if err != nil {
		t.Fatal(err)
	}
	base := []string{
		"idu: analyze file systems to create a database of per-file and aggregate size",
		"errors - list the contents of the errors database",
		"flag: help requested",
	}
	if err := containsAnyOf(out, base...); err != nil {
		t.Fatal(err)
	}
	out, _ = runIDU("--help")
	if err := containsAnyOf(out, base...); err != nil {
		t.Fatal(err)
	}
	err = containsAnyOf(out, "[--config=$HOME/.idu.yml --exit-profile= --gcpercent=50 --h=true --http= --units=decimal --v=0]")
	if err != nil {
		t.Fatal(err)
	}
	out, _ = runIDU("help", "analyze")

	err = containsAnyOf(out, "Usage of command analyze: analyze the file system to build a database of file counts, disk usage etc",
		" -incremental")
	if err != nil {
		t.Fatal(err)
	}
}
