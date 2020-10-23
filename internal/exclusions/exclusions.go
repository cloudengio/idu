// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package exclusions

import (
	"regexp"
	"sort"
	"strings"

	"cloudeng.io/cmd/idu/internal/config"
)

// T represents a set of exclusions as regular expressions.
type T struct {
	prefixes   []string
	exclusions [][]*regexp.Regexp
}

// New creates a new instance of exclusions.
func New(exclusions []config.Exclusions) *T {
	cpy := make([]config.Exclusions, len(exclusions))
	copy(cpy, exclusions)
	sort.Slice(cpy, func(i, j int) bool {
		return len(cpy[i].Prefix) > len(cpy[j].Prefix)
	})
	ex := &T{}
	for _, e := range cpy {
		ex.prefixes = append(ex.prefixes, e.Prefix)
		re := make([]*regexp.Regexp, len(e.Regexps))
		copy(re, e.Regexps)
		ex.exclusions = append(ex.exclusions, re)
	}
	return ex
}

// Exclude returns true if the supplied path matches any of the exclusions.
func (e T) Exclude(path string) bool {
	for i, p := range e.prefixes {
		if strings.HasPrefix(path, p) {
			for _, re := range e.exclusions[i] {
				if re.MatchString(path) {
					return true
				}
			}
		}
	}
	return false
}
