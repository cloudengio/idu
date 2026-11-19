// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package stats

import "fmt"

// Metric identifies one of the values recorded in Totals that prefixes,
// users and groups can be ranked by.
type Metric int

const (
	// MetricBytes ranks by Totals.Bytes.
	MetricBytes Metric = iota
	// MetricStorageBytes ranks by Totals.StorageBytes.
	MetricStorageBytes
	// MetricFiles ranks by Totals.Files.
	MetricFiles
	// MetricPrefixes ranks by Totals.SubPrefixes, ie. the number of
	// prefixes (directories) contained in a prefix.
	MetricPrefixes
	// MetricPrefixBytes ranks by Totals.PrefixBytes.
	MetricPrefixBytes
	// MetricSparseBytes ranks by Totals.SparseBytes.
	MetricSparseBytes
)

// NumMetrics is the number of defined Metrics.
const NumMetrics = int(MetricSparseBytes) + 1

// Metrics returns all of the defined Metrics.
func Metrics() []Metric {
	m := make([]Metric, NumMetrics)
	for i := range m {
		m[i] = Metric(i)
	}
	return m
}

var metricNames = [NumMetrics]string{
	"bytes", "storage-bytes", "files", "prefixes", "prefix-bytes", "sparse-bytes",
}

// String implements fmt.Stringer.
func (m Metric) String() string {
	if m < 0 || int(m) >= NumMetrics {
		return fmt.Sprintf("Metric(%d)", int(m))
	}
	return metricNames[m]
}

// ParseMetric returns the Metric whose String value is name.
func ParseMetric(name string) (Metric, error) {
	for i, n := range metricNames {
		if n == name {
			return Metric(i), nil
		}
	}
	return 0, fmt.Errorf("unknown metric: %q", name)
}

// Value returns the value of m in t.
func (m Metric) Value(t Totals) int64 {
	switch m {
	case MetricBytes:
		return t.Bytes
	case MetricStorageBytes:
		return t.StorageBytes
	case MetricFiles:
		return t.Files
	case MetricPrefixes:
		return t.SubPrefixes
	case MetricPrefixBytes:
		return t.PrefixBytes
	case MetricSparseBytes:
		return t.SparseBytes
	}
	return 0
}
