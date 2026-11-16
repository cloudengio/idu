// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package types

import (
	"bytes"
	"encoding/gob"
	"time"
)

type ErrorPayload struct {
	When    time.Time
	Key     string
	Payload []byte
}

type StatsPayload struct {
	When    time.Time
	Payload []byte
}

type LogPayload struct {
	Start, Stop time.Time
	Payload     []byte
}

func Decode[T any](buf []byte, v *T) error {
	dec := gob.NewDecoder(bytes.NewReader(buf))
	return dec.Decode(v)
}

func Encode[T any](buf *bytes.Buffer, v T) error {
	enc := gob.NewEncoder(buf)
	return enc.Encode(v)
}
