// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"text/template"
	"time"

	"cloudeng.io/cmd/idu/internal/reports"
)

func tpl(name string) *template.Template {
	return template.New(name).Funcs(template.FuncMap{
		"fmtBytes": fmtSize,
		"fmtCount": fmtCount,
	})
}

var mdTotals = template.Must(tpl("totals").Parse(`
## Totals for {{.Prefix}} as of {{.When}}

| Metric | Value |
| :--- | ---: |
| Bytes | {{fmtBytes .TotalBytes}} |
| Storage Bytes | {{fmtBytes .TotalStorageBytes}} |
| Files | {{fmtCount .TotalFiles }} |
| Prefixes | {{fmtCount .TotalPrefixes}} |

`))

var mdPrefixes = template.Must(tpl("prefixes").Parse(`
# Top {{.TopN}} prefixes by bytes used
| Bytes | Prefix |
| ---: | :--- |
{{range .Bytes}}| {{fmtBytes .Val}} | {{.Prefix}} |
{{end}}

{{if .StorageBytes}}
# Top {{.TopN}} prefixes by storage bytes usage
| Storage Bytes | Prefix |
| ---: | :--- |
{{range .StorageBytes}}| {{fmtBytes .Val}} |  {{.Prefix}} |
{{end}}
{{end}}

# Top {{.TopN}} prefixes by file count
| Files | Prefix |
| ---: | :--- |
{{range .Files}}| {{fmtCount .Val}} |  {{.Prefix}} |
{{end}}

# Top {{.TopN}} prefixes by prefix count
| Prefixes | Prefix |
| ---: | :--- |
{{range .Prefixes}}| {{fmtCount .Val}} |  {{.Prefix}} |
{{end}}

`))

type perPrefix struct {
	Val    int64
	Prefix string
}

type mdHeap struct {
	TopN         int
	Bytes        []perPrefix
	StorageBytes []perPrefix
	Files        []perPrefix
	Prefixes     []perPrefix
}

func zipPerPrefix(a []int64, b []string) []perPrefix {
	r := make([]perPrefix, len(a))
	for i := range a {
		r[i].Val = a[i]
		r[i].Prefix = b[i]
	}
	return r
}

type markdownReports struct {
}

func (md *markdownReports) generateReports(ctx context.Context, rf *reportsFlags, when time.Time, data []byte) error {
	var sdb reports.AllStats
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&sdb); err != nil {
		return err
	}
	out := &bytes.Buffer{}

	if err := mdTotals.Execute(out, struct {
		Heaps *reports.Heaps[string]
		When  string
	}{
		Heaps: sdb.Prefix,
		When:  when.Format(time.RFC3339),
	}); err != nil {
		return err
	}

	h := sdb.Prefix
	b, bp := h.PopAll(h.Bytes, rf.Markdown)
	fb, fbp := h.PopAll(h.Files, rf.Markdown)
	db, dbp := h.PopAll(h.Prefixes, rf.Markdown)
	mdh := mdHeap{
		TopN:     rf.Markdown,
		Bytes:    zipPerPrefix(b, bp),
		Files:    zipPerPrefix(fb, fbp),
		Prefixes: zipPerPrefix(db, dbp),
	}
	if h.StorageBytes != nil {
		sb, sbp := h.PopAll(h.StorageBytes, rf.Markdown)
		mdh.StorageBytes = zipPerPrefix(sb, sbp)
	}

	if err := mdPrefixes.Execute(out, mdh); err != nil {
		return err
	}

	fmt.Printf("%v\n", out.String())
	return nil

}
