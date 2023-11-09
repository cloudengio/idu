// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"cloudeng.io/algo/container/heap"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/internal/reports"
	"cloudeng.io/errors"
	"cloudeng.io/file"
	"cloudeng.io/file/diskusage"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/filewalk/asyncstat"
	"cloudeng.io/file/filewalk/localfs"
)

type duCmd struct{}

type duFlags struct {
	TopN                    int  `subcmd:"top-n,20,number of top entries to track/display"`
	ConcurrentScans         int  `subcmd:"concurrent-scans,100,number of concurrent directory scans"`
	ConcurrentStats         int  `subcmd:"concurrent-stats,1000,total number of concurrent lstat system calls to allow"`
	ConcurrentStatThreshold int  `subcmd:"concurrent-stat-threshold,10,threshold at which to start issuing concurrent lstat system calls"`
	ScanSize                int  `subcmd:"scan-size,1000,number of entries to scan per directory"`
	Defaults                bool `subcmd:"show-defaults,false,display default scanning options and exit"`
	Progress                bool `subcmd:"progress,true,show progress"`
	JSONReport              bool `subcmd:"json,false,generate json output"`
}

func (du *duCmd) du(ctx context.Context, values interface{}, args []string) error {
	df := values.(*duFlags)
	dir := args[0]

	if df.Defaults {
		showDefaults()
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	w := &duWalker{
		fs:        localfs.New(),
		pt:        newProgressTracker(ctx, time.Second, df.Progress, false, &wg),
		stats:     reports.NewAllStats(dir, false, df.TopN),
		slowScans: map[string]time.Duration{},
	}

	w.lsi = asyncstat.New(w.fs,
		asyncstat.WithAsyncStats(df.ConcurrentStats),
		asyncstat.WithAsyncThreshold(df.ConcurrentStatThreshold),
		asyncstat.WithLatencyTracker(w.pt),
		asyncstat.WithErrorLogger(w.logError))

	walkerStatus := make(chan filewalk.Status, 1000)

	walker := filewalk.New(w.fs,
		w,
		filewalk.WithConcurrentScans(df.ConcurrentScans),
		filewalk.WithScanSize(df.ScanSize),
		filewalk.WithReporting(walkerStatus, time.Second, time.Second*10),
	)

	go w.status(ctx, walkerStatus)

	errs := errors.M{}
	errs.Append(walker.Walk(ctx, dir))

	cancel()
	wg.Wait()

	w.stats.Finalize()

	if df.JSONReport {
		errs.Append(du.json(w, df, dir))
	} else {
		du.text(w, df, dir)
	}
	return errs.Err()
}

type slowScan struct {
	prefix string
	took   time.Duration
}

func (du *duCmd) slowScansList(slow map[string]time.Duration) []slowScan {
	out := []slowScan{}
	sk := heap.NewMinMax[time.Duration, string]()
	for k, v := range slow {
		sk.PushMaxN(v, k, len(slow))
	}
	for sk.Len() > 0 {
		k, v := sk.PopMax()
		out = append(out, slowScan{v, k})
	}
	return out
}

func (du *duCmd) text(w *duWalker, df *duFlags, dir string) {
	fmt.Println()

	// Print errors.
	banner(os.Stdout, "=", "Errors\n")
	for _, e := range w.duErrs {
		if !errors.Is(e.err, context.Canceled) {
			fmt.Fprintf(os.Stdout, "%v: %v\n", e.filename, e.err)
		}
	}

	fmt.Println()
	// Print slow scans
	banner(os.Stdout, "=", "Slow Scans\n")
	slowScans := du.slowScansList(w.slowScans)
	for _, s := range slowScans {
		fmt.Fprintf(os.Stdout, "%v: %v\n", s.prefix, s.took)
	}

	fmt.Println()

	heapFormatter[string]{}.formatTotals(w.stats.Prefix, os.Stdout)

	banner(os.Stdout, "=", "Usage by top %v Prefixes\n", df.TopN)
	heapFormatter[string]{}.formatHeaps(w.stats.Prefix, os.Stdout, func(v string) string { return v }, df.TopN)

	banner(os.Stdout, "=", "\nUsage by top %v users\n", df.TopN)
	heapFormatter[uint32]{}.formatHeaps(w.stats.ByUser, os.Stdout, globalUserManager.nameForUID, df.TopN)

	banner(os.Stdout, "=", "\nUsage by top %v groups\n", df.TopN)
	heapFormatter[uint32]{}.formatHeaps(w.stats.ByGroup, os.Stdout, globalUserManager.nameForGID, df.TopN)

}

type jsonHeapPrefix struct {
	TopPrefixesSize []struct {
		Bytes  int64  `json:"bytes"`
		Prefix string `json:"prefix"`
	} `json:"top_prefixes_size"`
	TopPrefixesFiles []struct {
		Files  int64  `json:"files"`
		Prefix string `json:"prefix"`
	} `json:"top_prefixes_files"`
	TopPrefixesChildren []struct {
		Children int64  `json:"children"`
		Prefix   string `json:"prefix"`
	} `json:"top_prefixes_children"`
}

func (jh *jsonHeapPrefix) fill(h *reports.Heaps[string]) {
	for h.Bytes.Len() > 0 {
		k, v := h.Bytes.PopMax()
		jh.TopPrefixesSize = append(jh.TopPrefixesSize, struct {
			Bytes  int64  `json:"bytes"`
			Prefix string `json:"prefix"`
		}{k, v})
	}
	for h.Files.Len() > 0 {
		k, v := h.Files.PopMax()
		jh.TopPrefixesFiles = append(jh.TopPrefixesFiles, struct {
			Files  int64  `json:"files"`
			Prefix string `json:"prefix"`
		}{k, v})
	}
	for h.Prefixes.Len() > 0 {
		k, v := h.Prefixes.PopMax()
		jh.TopPrefixesChildren = append(jh.TopPrefixesChildren, struct {
			Children int64  `json:"children"`
			Prefix   string `json:"prefix"`
		}{k, v})
	}
}

type jsonHeapID struct {
	TopUserGroupSize []struct {
		Bytes int64  `json:"bytes"`
		ID    uint32 `json:"id"`
		Name  string `json:"name"`
	} `json:"top_prefixes_size"`
	TopUserGroupFiles []struct {
		Files int64  `json:"files"`
		ID    uint32 `json:"id"`
		Name  string `json:"name"`
	} `json:"top_prefixes_files"`
	TopUserGroupChildren []struct {
		Children int64  `json:"children"`
		ID       uint32 `json:"id"`
		Name     string `json:"name"`
	} `json:"top_prefixes_children"`
}

func (ji *jsonHeapID) fill(h *reports.Heaps[uint32], fn func(uint32) string) {
	for h.Bytes.Len() > 0 {
		k, v := h.Bytes.PopMax()
		ji.TopUserGroupSize = append(ji.TopUserGroupSize, struct {
			Bytes int64  `json:"bytes"`
			ID    uint32 `json:"id"`
			Name  string `json:"name"`
		}{k, v, fn(v)})
	}
	for h.Files.Len() > 0 {
		k, v := h.Files.PopMax()
		ji.TopUserGroupFiles = append(ji.TopUserGroupFiles, struct {
			Files int64  `json:"files"`
			ID    uint32 `json:"id"`
			Name  string `json:"name"`
		}{k, v, fn(v)})
	}
	for h.Prefixes.Len() > 0 {
		k, v := h.Prefixes.PopMax()
		ji.TopUserGroupChildren = append(ji.TopUserGroupChildren, struct {
			Children int64  `json:"children"`
			ID       uint32 `json:"id"`
			Name     string `json:"name"`
		}{k, v, fn(v)})
	}
}

type jsonOutput struct {
	Errors    []struct{ Prefix, Error string } `json:"errors,omit_empty"`
	SlowScans []struct {
		Prefix   string
		Duration time.Duration
	} `json:"slow_scans,omit_empty"`
	TotalBytes    int64 `json:"total_bytes"`
	TotalFiles    int64 `json:"total_files"`
	TotalPrefixes int64 `json:"total_prefixes"`
	jsonHeapPrefix

	Users  jsonHeapID `json:"top_users"`
	Groups jsonHeapID `json:"top_groups"`
}

func (du *duCmd) json(w *duWalker, df *duFlags, dir string) error {
	out := jsonOutput{}
	for _, e := range w.duErrs {
		out.Errors = append(out.Errors, struct{ Prefix, Error string }{e.filename, e.err.Error()})
	}
	slowScans := du.slowScansList(w.slowScans)
	for _, e := range slowScans {
		out.SlowScans = append(out.SlowScans, struct {
			Prefix   string
			Duration time.Duration
		}{e.prefix, e.took})
	}

	out.TotalBytes = w.stats.Prefix.TotalBytes
	out.TotalFiles = w.stats.Prefix.TotalFiles
	out.TotalPrefixes = w.stats.Prefix.TotalPrefixes

	out.jsonHeapPrefix.fill(w.stats.Prefix)
	out.Users.fill(w.stats.ByUser, globalUserManager.nameForUID)
	out.Groups.fill(w.stats.ByGroup, globalUserManager.nameForGID)

	buf, err := json.Marshal(out)
	if err != nil {
		return err
	}
	fmt.Println(string(buf))
	return nil
}

type duError struct {
	filename string
	err      error
}

type duWalker struct {
	fs        filewalk.FS
	pt        *progressTracker
	lsi       *asyncstat.T
	slowScans map[string]time.Duration
	statsMu   sync.Mutex
	stats     *reports.AllStats
	duErrsMu  sync.Mutex
	duErrs    []duError
}

type duState struct {
	current prefixinfo.T
	nfiles  int64
}

func (w *duWalker) status(ctx context.Context, ch <-chan filewalk.Status) {
	for {
		select {
		case <-ctx.Done():
			return
		case s := <-ch:
			w.pt.setSyncScans(s.SynchronousScans)
			if s.SlowPrefix != "" {
				w.slowScans[s.SlowPrefix] = s.ScanDuration
			}
		}
	}
}

func (w *duWalker) logError(ctx context.Context, filename string, err error) {
	w.duErrsMu.Lock()
	defer w.duErrsMu.Unlock()
	w.pt.incErrors()
	w.duErrs = append(w.duErrs, duError{filename, err})
}

func (w *duWalker) Prefix(ctx context.Context, state *duState, prefix string, info file.Info, err error) (stop bool, _ file.InfoList, retErr error) {
	if err != nil {
		w.pt.incErrors()
		return true, nil, err
	}
	w.pt.incStartPrefix()
	pi, err := prefixinfo.New(info)
	if err != nil {
		return true, nil, err
	}
	state.current = pi
	return false, nil, nil
}

func (w *duWalker) Contents(ctx context.Context, state *duState, prefix string, contents []filewalk.Entry) (file.InfoList, error) {
	children, all, err := w.lsi.Process(ctx, prefix, contents)
	state.nfiles += int64(len(all) - len(children))
	state.current.AppendInfoList(all)
	return children, err
}

func (w *duWalker) Done(ctx context.Context, state *duState, prefix string, err error) error {
	if err != nil {
		w.pt.incErrors()
		w.logError(ctx, prefix, err)
	}
	if err := state.current.Finalize(); err != nil {
		w.logError(ctx, prefix, fmt.Errorf("failed to finalize %v\n", err))
		return err
	}
	w.pt.incDonePrefix(0, state.nfiles)
	w.statsMu.Lock()
	defer w.statsMu.Unlock()
	if err := w.stats.Update(prefix, state.current, diskusage.Identity{}); err != nil {
		w.logError(ctx, prefix, fmt.Errorf("failed to update stats: %v\n", err))
	}
	return nil
}
