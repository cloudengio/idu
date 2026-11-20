// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package cli

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"cloudeng.io/errors"
)

func TestSquashCanceled(t *testing.T) {
	other := fmt.Errorf("permission denied")
	// A walk's errors nested within a single-error errors.M, as returned by
	// analyze, which errors.M.Squash alone does not squash.
	walk := &errors.M{}
	for i := 0; i < 1000; i++ {
		walk.Append(fmt.Errorf("prefix %v: %w", i, context.Canceled))
	}
	walk.Append(other)
	for i := 0; i < 10; i++ {
		walk.Append(context.DeadlineExceeded)
	}
	analyzer := &errors.M{}
	analyzer.Append(walk)
	top := &errors.M{}
	top.Append(analyzer)

	if strings.Count(top.Squash(context.Canceled).Error(), "context canceled") != 1000 {
		t.Fatalf("errors.M.Squash behavior has changed, SquashCanceled may no longer be needed")
	}

	err := SquashCanceled(top)
	msg := err.Error()
	if got := strings.Count(msg, "context canceled"); got != 1 {
		t.Errorf("got %v instances of context canceled: %v", got, msg)
	}
	for _, want := range []string{"repeated 1000 times", "repeated 10 times", "permission denied"} {
		if !strings.Contains(msg, want) {
			t.Errorf("%q does not contain %q", msg, want)
		}
	}
	for _, target := range []error{context.Canceled, context.DeadlineExceeded, other} {
		if !errors.Is(err, target) {
			t.Errorf("errors.Is(%v) is false", target)
		}
	}

	if SquashCanceled(nil) != nil || SquashCanceled(&errors.M{}) != nil {
		t.Errorf("expected nil")
	}
	if got := SquashCanceled(other); got != other {
		t.Errorf("got %v, want %v", got, other)
	}

	wrapped := fmt.Errorf("wrapped: %w", top)
	squashedWrapped := SquashCanceled(wrapped)
	if got := strings.Count(squashedWrapped.Error(), "context canceled"); got != 1 {
		t.Errorf("wrapped: got %v instances of context canceled: %v", got, squashedWrapped)
	}
	if !errors.Is(squashedWrapped, other) {
		t.Errorf("wrapped: errors.Is(other) is false")
	}
}
