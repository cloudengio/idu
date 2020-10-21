// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package exclusions_test

import (
	"fmt"
	"testing"

	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/cmd/idu/internal/exclusions"
)

const cfg = `databases:
  - prefix: /
    type: local
    directory: /dev/null
exclusions:
  - prefix: /
    regexps:
      - "^/a/b/c$"
  - prefix: /tmp
    regexps:
      - "/z/"
`

func TestExclusions(t *testing.T) {
	cfg, err := config.ParseConfig([]byte(cfg))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("EX: %#v\n", cfg.Exclusions)
	ex := exclusions.New(cfg.Exclusions)
	for i, tc := range []struct {
		path    string
		matched bool
	}{
		{"/a/b/c", true},
		{"/tmp/a/b/c", false},
		{"/a/b/cc", false},
		{"/tmp/a/b/cc", false},
		{"a/z/", false},
		{"/tmp/a/z/", true},
		{"/z/b", false},
		{"/tmp//z/b", true},
		{"a/z", false},
		{"/tmp/a/z", false},
		{"a", false},
		{"/tmp/a", false},
	} {
		if got, want := ex.Exclude(tc.path), tc.matched; got != want {
			t.Errorf("%v; %v: got %v, want %v", i, tc.path, got, want)
		}
	}
}
