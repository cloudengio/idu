// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package types_test

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/idudb/backends/badgerdb/internal/types"
)

func assertGobRoundTrip[T any](t *testing.T, in T) {
	t.Helper()
	var buf bytes.Buffer
	if err := types.Encode(&buf, in); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var out T
	if err := types.Decode(buf.Bytes(), &out); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Errorf("got %+v, want %+v", out, in)
	}
}

func TestEncodeDecode(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	t.Run("ErrorPayload", func(t *testing.T) {
		t.Parallel()
		assertGobRoundTrip(t, types.ErrorPayload{
			When:    now,
			Key:     "/a/b",
			Payload: []byte("permission denied"),
		})
	})

	t.Run("StatsPayload", func(t *testing.T) {
		t.Parallel()
		assertGobRoundTrip(t, types.StatsPayload{
			When:    now,
			Payload: []byte("stats-bytes"),
		})
	})

	t.Run("LogPayload", func(t *testing.T) {
		t.Parallel()
		assertGobRoundTrip(t, types.LogPayload{
			Start:   now,
			Stop:    now.Add(5 * time.Minute),
			Payload: []byte(`{"operation":"analyze"}`),
		})
	})

	t.Run("corrupt buffer", func(t *testing.T) {
		t.Parallel()
		var out types.LogPayload
		if err := types.Decode([]byte{0xff, 0x00, 0x01}, &out); err == nil {
			t.Fatal("expected error decoding corrupt buffer")
		}
	})
}
