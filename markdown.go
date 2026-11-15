// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"fmt"
	"os"
	"text/template"
	"time"

	"cloudeng.io/cmd/idu/internal/reports"
	"cloudeng.io/cmd/idu/internal/usernames"
	"cloudeng.io/file/diskusage"
	"golang.org/x/exp/maps"
)

func tpl(name string) *template.Template {
	return template.New(name).Funcs(template.FuncMap{
		"fmtBytes":   fmtSize,
		"fmtKiBytes": fmtKiBytes,
		"fmtCount":   fmtCount,
		"fmtAmp":     func(v float64) string { return fmt.Sprintf("%.1fx", v) },
		"fmtPct":     func(v float64) string { return fmt.Sprintf("%.0f%%", v*100) },
	})
}

func fmtKiBytes(size int64) string {
	size /= int64(diskusage.KiB)
	return printer.Sprintf("%v KiB", size)
}

const mdTOC = `
# Filesystem Usage Reports for {{.Prefix}} {{.Expression}}

## Contents

* [Totals](#totals)
* [Top {{.TopN}} prefixes](#top-prefixes)
* [Top {{.TopN}} subtrees](#top-subtrees)
* [Distributed subtrees](#distributed-subtrees)
* [Top {{.TopN}} users](#top-Users)
* [Top {{.TopN}} groups](#top-Groups)

`

const mdTotals = `
# <a id=totals></a> Totals for {{.Prefix}} as of {{.When}}

| Metric | Value |
| :--- | ---: |
| Bytes | {{fmtBytes .Heaps.TotalBytes}} ({{fmtKiBytes .Heaps.TotalBytes}}) |
| Storage Bytes | {{fmtBytes .Heaps.TotalStorageBytes}} ({{fmtKiBytes .Heaps.TotalStorageBytes}}) |
| Files | {{fmtCount .Heaps.TotalFiles }} |
| Prefixes | {{fmtCount .Heaps.TotalPrefixes}} |
| Prefixes + Files | {{fmtCount .TotalPrefixesAndFiles}} |
| Prefix Bytes | {{fmtBytes .Heaps.TotalPrefixBytes}} |
| Sparse Files | {{fmtCount .Heaps.TotalSparseFiles}} |
| Sparse Bytes (not allocated) | {{fmtBytes .Heaps.TotalSparseBytes}} |

`

const mdPrefixes = `
# <a id=top-prefixes></a> Top {{.TopN}} prefixes for {{.Prefix}}

### Top {{.TopN}} prefixes by bytes used
| Bytes | Prefix |
| ---: | :--- |
{{range .Bytes}}| {{fmtBytes .K}} | {{.V}} |
{{end}}

{{if .StorageBytes}}
### Top {{.TopN}} prefixes by storage bytes usage
| Storage Bytes | Prefix |
| ---: | :--- |
{{range .StorageBytes}}| {{fmtBytes .K}} |  {{.V}} |
{{end}}
{{end}}

### Top {{.TopN}} prefixes by file count
| Files | Prefix |
| ---: | :--- |
{{range .Files}}| {{fmtCount .K}} |  {{.V}} |
{{end}}

### Top {{.TopN}} prefixes by prefix count
| Prefixes | Prefix |
| ---: | :--- |
{{range .Prefixes}}| {{fmtCount .K}} |  {{.V}} |
{{end}}

### Top {{.TopN}} prefixes by prefix bytes
| Prefixes | Prefix |
| ---: | :--- |
{{range .PrefixBytes}}| {{fmtBytes .K}} |  {{.V}} |
{{end}}

{{if .SparseBytes}}
### Top {{.TopN}} prefixes by sparse bytes not allocated on disk
| Sparse Bytes | Prefix |
| ---: | :--- |
{{range .SparseBytes}}| {{fmtBytes .K}} |  {{.V}} |
{{end}}
{{end}}

`

const mdSubtrees = `
# <a id=top-subtrees></a> Top {{.TopN}} subtrees (recursive) for {{.Prefix}}

### Top {{.TopN}} subtrees by bytes used
| Bytes | Subtree |
| ---: | :--- |
{{range .Bytes}}| {{fmtBytes .K}} | {{.V}} |
{{end}}

{{if .StorageBytes}}
### Top {{.TopN}} subtrees by storage bytes usage
| Storage Bytes | Subtree |
| ---: | :--- |
{{range .StorageBytes}}| {{fmtBytes .K}} |  {{.V}} |
{{end}}
{{end}}

### Top {{.TopN}} subtrees by file count
| Files | Subtree |
| ---: | :--- |
{{range .Files}}| {{fmtCount .K}} |  {{.V}} |
{{end}}

### Top {{.TopN}} subtrees by directory count
| Directories | Subtree |
| ---: | :--- |
{{range .Prefixes}}| {{fmtCount .K}} |  {{.V}} |
{{end}}

{{if .SparseBytes}}
### Top {{.TopN}} subtrees by sparse bytes not allocated on disk
| Sparse Bytes | Subtree |
| ---: | :--- |
{{range .SparseBytes}}| {{fmtBytes .K}} |  {{.V}} |
{{end}}
{{end}}

`

const mdDistributed = `
# <a id=distributed-subtrees></a> Distributed subtree hotspots (modest fanout) for {{.Prefix}}

| Distributed Storage | Subtree Storage | Max Leaf Storage | Amp | Dirs | Max Child | Prefix |
| ---: | ---: | ---: | ---: | ---: | ---: | :--- |
{{range .Items}}| {{fmtBytes .DistributedStorage}} | {{fmtBytes .SubtreeStorage}} | {{fmtBytes .MaxLeafStorage}} | {{fmtAmp .StorageAmplification}} | {{fmtCount .SubtreeDirs}} | {{fmtPct .MaxChildShare}} | {{.Prefix}} |
{{end}}

`

const mdListUsersAndGroups = `
# Per User Reports - click on a link below
{{range $idx, $u := .Users}}{{if $idx}}, {{end}}[{{fmtUID .}}](#user-{{.}}){{end}}

# Per Group Reports - click on a link below
{{range $idx, $g := .Groups}}{{if $idx}}, {{end}}[{{fmtGID .}}](#group-{{.}}){{end}}  
`

const mdByUsersGroupsTemplate = `
# <a id=top-{{.UserOrGroup}}></a> Top {{.TopN}} {{.UserOrGroup}} for {{.Prefix}}

## Top {{.TopN}} {{.UserOrGroup}} by bytes used
| Bytes | User |
| ---: | :--- |
{{range .Bytes}}| {{fmtBytes .K}} | {{fmtID .V}} |
{{end}}

{{if .StorageBytes}}
## Top {{.TopN}} {{.UserOrGroup}} by storage bytes usage
| Storage Bytes | Prefix |
| ---: | :--- |
{{range .StorageBytes}}| {{fmtBytes .K}} | {{fmtID .V}} |
{{end}}
{{end}}

## Top {{.TopN}} {{.UserOrGroup}} by file count
| Files | Prefix |
| ---: | :--- |
{{range .Files}}| {{fmtCount .K}} | {{fmtID .V}} |
{{end}}

## Top {{.TopN}} {{.UserOrGroup}} by prefix count
| Prefixes | Prefix |
| ---: | :--- |
{{range .Prefixes}}| {{fmtCount .K}} | {{fmtID .V}} |
{{end}}

## Top {{.TopN}} {{.UserOrGroup}} by prefix bytes
| Prefix Bytes | Prefix |
| ---: | :--- |
{{range .PrefixBytes}}| {{fmtBytes .K}} | {{fmtID .V}} |
{{end}}
`

const mdPerUsersGroupsTemplate = `
# <a id=per-{{.UserOrGroup}}></a> Usage per {{.UserOrGroup}} for {{.Prefix}}

{{range $id, $heap := .PerID}}

## <a id="{{.UserOrGroup}}-{{$id}}"></a> {{.UserOrGroup}} {{fmtID $id}}

## {{.UserOrGroup}} {{fmtID $id}}: top {{.TopN}} prefixes by bytes used
| Bytes | User |
| ---: | :--- |
{{range .Bytes}}| {{fmtBytes .K}} | {{.V}} |
{{end}}

{{if .StorageBytes}}
## {{.UserOrGroup}} {{fmtID $id}}: top {{.TopN}} prefixes by storage bytes usage
| Storage Bytes | Prefix |
| ---: | :--- |
{{range .StorageBytes}}| {{fmtBytes .K}} | {{.V}} |
{{end}}
{{end}}

## {{.UserOrGroup}} {{fmtID $id}}: top {{.TopN}} prefixes by file count
| Files | Prefix |
| ---: | :--- |
{{range .Files}}| {{fmtCount .K}} | {{.V}} |
{{end}}

## {{.UserOrGroup}} {{fmtID $id}}: top {{.TopN}} prefixes by prefix count
| Prefixes | Prefix |
| ---: | :--- |
{{range .Prefixes}}| {{fmtCount .K}} | {{.V}} |
{{end}}

## {{.UserOrGroup}} {{fmtID $id}}: top {{.TopN}} prefixes by prefix bytes
| Prefix Bytes | Prefix |
| ---: | :--- |
{{range .Prefixes}}| {{fmtBytes .K}} | {{.V}} |
{{end}}

{{end}}
`

type mdHeap[T comparable] struct {
	TopN         int
	Prefix       string
	UserOrGroup  string
	Bytes        []reports.Zipped[T]
	StorageBytes []reports.Zipped[T]
	Files        []reports.Zipped[T]
	Prefixes     []reports.Zipped[T]
	PrefixBytes  []reports.Zipped[T]
	SparseBytes  []reports.Zipped[T]
}

type mdPerIDHeap struct {
	TopN        int
	Prefix      string
	UserOrGroup string
	PerID       map[int64]mdHeap[string]
}

type markdownReports struct {
	created     bool
	toc         *template.Template
	lists       *template.Template
	totals      *template.Template
	prefixes    *template.Template
	subtrees    *template.Template
	distributed *template.Template
	byUsers     *template.Template
	byGroups    *template.Template
	perUsers    *template.Template
	perGroups   *template.Template
}

func (md *markdownReports) initTemplates() {
	if md.created {
		return
	}
	nameForUID := usernames.Manager.NameForUID
	nameForGID := usernames.Manager.NameForGID
	md.toc = template.Must(tpl("toc").Parse(mdTOC))
	md.totals = template.Must(tpl("totals").Parse(mdTotals))
	md.prefixes = template.Must(tpl("prefixes").Parse(mdPrefixes))
	md.subtrees = template.Must(tpl("subtrees").Parse(mdSubtrees))
	md.distributed = template.Must(tpl("distributed").Parse(mdDistributed))
	md.lists = template.Must(tpl("userGroupLists").Funcs(
		template.FuncMap{
			"fmtUID": nameForUID,
			"fmtGID": nameForGID,
		}).Parse(mdListUsersAndGroups))

	md.byUsers = template.Must(tpl("users").
		Funcs(template.FuncMap{"fmtID": nameForUID}).
		Parse(mdByUsersGroupsTemplate))

	md.byGroups = template.Must(tpl("groups").
		Funcs(template.FuncMap{"fmtID": nameForGID}).
		Parse(mdByUsersGroupsTemplate))

	md.perUsers = template.Must(tpl("users").Funcs(
		template.FuncMap{"fmtID": nameForUID}).
		Parse(mdPerUsersGroupsTemplate))

	md.perGroups = template.Must(tpl("users").Funcs(
		template.FuncMap{"fmtID": nameForGID}).
		Parse(mdPerUsersGroupsTemplate))

	md.created = true
}

func (md *markdownReports) generateReports(rf *generateReportsFlags, filenames *reportFilenames, stats statsFileFormat) error {
	md.initTemplates()

	prefix := stats.Prefix
	when := stats.Date
	sdb := stats.Stats
	out := &bytes.Buffer{}

	// TOC & user/group links

	if err := md.toc.Execute(out, struct {
		Prefix     string
		Expression string
		TopN       int
		When       string
	}{
		Prefix:     prefix,
		Expression: stats.Expression,
		TopN:       rf.Markdown,
		When:       when.Format(time.RFC3339),
	}); err != nil {
		return err
	}

	uids, gids := maps.Keys(sdb.PerUser.ByPrefix), maps.Keys(sdb.PerGroup.ByPrefix)
	if err := md.lists.Execute(out, struct {
		Users, Groups []int64
	}{
		Users:  uids,
		Groups: gids,
	}); err != nil {
		return err
	}

	// Totals.
	if err := md.totals.Execute(out, struct {
		Prefix                string
		Heaps                 *reports.Heaps[string]
		TotalPrefixesAndFiles int64
		When                  string
	}{
		Prefix:                prefix,
		Heaps:                 sdb.Prefix,
		TotalPrefixesAndFiles: sdb.Prefix.TotalPrefixes + sdb.Prefix.TotalFiles,
		When:                  when.Format(time.RFC3339),
	}); err != nil {
		return err
	}

	// Largest Prefixes
	byPrefix := mdHeap[string]{
		Prefix:       prefix,
		TopN:         rf.Markdown,
		Bytes:        reports.ZipN(sdb.Prefix.Bytes, rf.Markdown),
		Files:        reports.ZipN(sdb.Prefix.Files, rf.Markdown),
		Prefixes:     reports.ZipN(sdb.Prefix.Prefixes, rf.Markdown),
		StorageBytes: reports.ZipN(sdb.Prefix.StorageBytes, rf.Markdown),
		PrefixBytes:  reports.ZipN(sdb.Prefix.PrefixBytes, rf.Markdown),
		SparseBytes:  reports.ZipN(sdb.Prefix.SparseBytes, rf.Markdown),
	}

	if err := md.prefixes.Execute(out, byPrefix); err != nil {
		return err
	}

	if sdb.Subtree != nil {
		bySubtree := mdHeap[string]{
			Prefix:       prefix,
			TopN:         rf.Markdown,
			Bytes:        reports.ZipN(sdb.Subtree.Bytes, rf.Markdown),
			Files:        reports.ZipN(sdb.Subtree.Files, rf.Markdown),
			Prefixes:     reports.ZipN(sdb.Subtree.Prefixes, rf.Markdown),
			StorageBytes: reports.ZipN(sdb.Subtree.StorageBytes, rf.Markdown),
			PrefixBytes:  reports.ZipN(sdb.Subtree.PrefixBytes, rf.Markdown),
			SparseBytes:  reports.ZipN(sdb.Subtree.SparseBytes, rf.Markdown),
		}
		if err := md.subtrees.Execute(out, bySubtree); err != nil {
			return err
		}
	}

	if dist := reports.MergeDistributed(sdb.Distributed, rf.Markdown); len(dist) > 0 {
		if err := md.distributed.Execute(out, struct {
			Prefix string
			Items  []reports.DistributedStats
		}{
			Prefix: prefix,
			Items:  dist,
		}); err != nil {
			return err
		}
	}

	// Largest Users/Groups.
	byUsers := mdHeap[int64]{
		Prefix:       prefix,
		TopN:         rf.Markdown,
		UserOrGroup:  "Users",
		Bytes:        reports.ZipN(sdb.ByUser.Bytes, rf.Markdown),
		Files:        reports.ZipN(sdb.ByUser.Files, rf.Markdown),
		Prefixes:     reports.ZipN(sdb.ByUser.Prefixes, rf.Markdown),
		StorageBytes: reports.ZipN(sdb.ByUser.StorageBytes, rf.Markdown),
		PrefixBytes:  reports.ZipN(sdb.ByUser.PrefixBytes, rf.Markdown),
	}

	if err := md.byUsers.Execute(out, byUsers); err != nil {
		return err
	}

	byGroups := mdHeap[int64]{
		Prefix:       prefix,
		TopN:         rf.Markdown,
		UserOrGroup:  "Groups",
		Bytes:        reports.ZipN(sdb.ByGroup.Bytes, rf.Markdown),
		Files:        reports.ZipN(sdb.ByGroup.Files, rf.Markdown),
		Prefixes:     reports.ZipN(sdb.ByGroup.Prefixes, rf.Markdown),
		StorageBytes: reports.ZipN(sdb.ByGroup.StorageBytes, rf.Markdown),
		PrefixBytes:  reports.ZipN(sdb.ByGroup.PrefixBytes, rf.Markdown),
	}

	if err := md.byGroups.Execute(out, byGroups); err != nil {
		return err
	}

	for _, r := range []struct {
		label string
		tpl   *template.Template
		data  map[int64]*reports.Heaps[string]
	}{
		{"user", md.perUsers, sdb.PerUser.ByPrefix},
		{"group", md.perGroups, sdb.PerGroup.ByPrefix},
	} {
		perUsers := mdPerIDHeap{
			TopN:        rf.Markdown,
			Prefix:      prefix,
			UserOrGroup: r.label,
			PerID:       make(map[int64]mdHeap[string]),
		}
		for id, us := range r.data {
			perUsers.PerID[id] = mdHeap[string]{
				Prefix:       prefix,
				TopN:         rf.Markdown,
				UserOrGroup:  r.label,
				Bytes:        reports.ZipN(us.Bytes, rf.Markdown),
				Files:        reports.ZipN(us.Files, rf.Markdown),
				Prefixes:     reports.ZipN(us.Prefixes, rf.Markdown),
				StorageBytes: reports.ZipN(us.StorageBytes, rf.Markdown),
				PrefixBytes:  reports.ZipN(us.PrefixBytes, rf.Markdown),
			}
		}
		if err := r.tpl.Execute(out, perUsers); err != nil {
			return err
		}
	}
	return os.WriteFile(filenames.summary("usage-summary"), out.Bytes(), 0660) //nolint:gosec
}
