// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal_test

import (
	"context"
	"flag"
	"os"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/cmdutil/flags"
)

func TestTimeRange(t *testing.T) {

	parse := func(args ...string) (time.Time, time.Time) {
		fs := &flag.FlagSet{}
		var trf internal.TimeRangeFlags
		if err := flags.RegisterFlagsInStruct(fs, "subcmd", &trf, nil, nil); err != nil {
			t.Fatal(err)
		}
		if err := fs.Parse(args); err != nil {
			t.Fatal(err)
		}
		from, to, err := trf.FromTo()
		if err != nil {
			t.Fatal(err)
		}
		return from, to
	}

	from, to := parse("--since=1h")
	if got := to.Sub(from); got < time.Minute*55 || got > time.Hour {
		t.Errorf("out of plausible range %v", got)
	}
	from, to = parse("--from=2020-10-10", "-to=2020-10-11")
	if got, want := to.Sub(from), time.Hour*24; got != want {
		t.Errorf("got %v, want 24h", got)
	}

}

const simple = `- prefix: /tmp
- prefix: /
`

func TestPrefixLookup(t *testing.T) {
	ctx := context.Background()
	cfg, err := config.ParseConfig([]byte(simple))
	if err != nil {
		t.Fatal(err)
	}

	ctx, prefix, err := internal.LookupPrefix(ctx, cfg, "/tmp")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := prefix.Prefix, "/tmp"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	ctx, prefix, err = internal.LookupPrefix(ctx, cfg, "/tmp/xx")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := prefix.Prefix, "/tmp"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestPrefixLookupRelative(t *testing.T) {
	ctx := context.Background()
	pwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []string{".", "", "./"} {
		here := `- prefix: ` + pwd + "\n"
		cfg, err := config.ParseConfig([]byte(here))
		if err != nil {
			t.Fatal(err)
		}
		ctx, prefix, err := internal.LookupPrefix(ctx, cfg, tc)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := prefix.Prefix, pwd; got != want {
			t.Errorf("got %v, want %v", got, want)
		}

		ctx, prefix, err = internal.LookupPrefix(ctx, cfg, "config")
		if err != nil {
			t.Fatal(err)
		}
		if got, want := prefix.Prefix, pwd; got != want {
			t.Errorf("got %v, want %v", got, want)
		}

	}
}
