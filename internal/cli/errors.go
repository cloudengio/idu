// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package cli

import (
	"context"
	stderrors "errors"

	"cloudeng.io/errors"
)

// SquashCanceled returns err with all but one of the instances of
// context.Canceled and context.DeadlineExceeded removed, so that a command
// that is interrupted, or times out, while many operations are in flight
// reports each once, together with the number of times that it occurred,
// rather than once per operation. Any errors.M within err, at any depth,
// are flattened into a single errors.M first since errors.M.Squash does not
// descend into an errors.M that contains a single error.
func SquashCanceled(err error) error {
	if err == nil {
		return nil
	}
	var m *errors.M
	if !stderrors.As(err, &m) {
		return err
	}
	flat := &errors.M{}
	flatten(flat, err)
	if flat.Err() == nil {
		return nil
	}
	return flat.Squash(context.Canceled, context.DeadlineExceeded)
}

func flatten(dst *errors.M, err error) {
	if u, ok := err.(interface{ Unwrap() []error }); ok {
		for _, e := range u.Unwrap() {
			flatten(dst, e)
		}
		return
	}
	var m *errors.M
	if stderrors.As(err, &m) {
		flatten(dst, m)
		return
	}
	dst.Append(err)
}
