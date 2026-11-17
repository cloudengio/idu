// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package badgerdb_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"cloudeng.io/cmd/idu/idudb/backends/badgerdb"
)

func TestLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	bl := badgerdb.NewLogger(logger)
	bl.Errorf("error %d\n", 1)
	bl.Warningf("warning %d\n", 2)
	bl.Infof("info %d\n", 3)
	bl.Debugf("debug %d\n", 4)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if got, want := len(lines), 2; got != want {
		t.Fatalf("got %v log lines, want %v: %q", got, want, buf.String())
	}
	for i, want := range []struct{ level, msg string }{
		{"ERROR", "error 1"},
		{"WARN", "warning 2"},
	} {
		var rec map[string]any
		if err := json.Unmarshal([]byte(lines[i]), &rec); err != nil {
			t.Fatal(err)
		}
		if rec[slog.LevelKey] != want.level || rec[slog.MessageKey] != want.msg || rec["component"] != "badger" {
			t.Errorf("line %v: got %v, want %v", i, rec, want)
		}
	}
}
