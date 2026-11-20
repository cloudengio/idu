// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package progress

import (
	"runtime/metrics"
)

// Memory describes the memory used by the process.
type Memory struct {
	// HeapBytes is the memory occupied by live, and not yet collected,
	// heap objects.
	HeapBytes uint64
	// TotalBytes is all of the memory mapped by the Go runtime.
	TotalBytes uint64
	// MaxRSSBytes is the peak resident set size of the process, it is
	// negative if unknown, eg. on Windows.
	MaxRSSBytes int64
	Goroutines  uint64
	GCCycles    uint64
}

var memoryMetrics = []string{
	"/memory/classes/heap/objects:bytes",
	"/memory/classes/total:bytes",
	"/sched/goroutines:goroutines",
	"/gc/cycles/total:gc-cycles",
}

// ReadMemory returns the memory currently used by the process. Unlike
// runtime.ReadMemStats it does not stop the world and hence may be called
// frequently.
func ReadMemory() Memory {
	samples := make([]metrics.Sample, len(memoryMetrics))
	for i, name := range memoryMetrics {
		samples[i].Name = name
	}
	metrics.Read(samples)
	value := func(i int) uint64 {
		if samples[i].Value.Kind() != metrics.KindUint64 {
			return 0
		}
		return samples[i].Value.Uint64()
	}
	var sys sysMemstats
	sys.update()
	return Memory{
		HeapBytes:   value(0),
		TotalBytes:  value(1),
		Goroutines:  value(2),
		GCCycles:    value(3),
		MaxRSSBytes: sys.maxRSSBytes(),
	}
}
