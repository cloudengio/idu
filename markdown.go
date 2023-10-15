// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"os"
	"text/template"
	"time"

	"cloudeng.io/cmd/idu/internal/reports"
	"golang.org/x/exp/maps"
)

func tpl(name string) *template.Template {
	return template.New(name).Funcs(template.FuncMap{
		"fmtBytes": fmtSize,
		"fmtCount": fmtCount,
	})
}

var mdTOC = template.Must(tpl("toc").Parse(`
# Filesystem Usage Reports for {{.Prefix}}

## Contents

* [Totals](#totals)
* [Top {{.TopN}} prefixes](#top-prefixes)
* [Top {{.TopN}} users](#top-Users)
* [Top {{.TopN}} groups](#top-Groups)

`))

var mdTotals = template.Must(tpl("totals").Parse(`
# <a id=totals></a> Totals for {{.Prefix}} as of {{.When}}

| Metric | Value |
| :--- | ---: |
| Bytes | {{fmtBytes .Heaps.TotalBytes}} |
| Storage Bytes | {{fmtBytes .Heaps.TotalStorageBytes}} |
| Files | {{fmtCount .Heaps.TotalFiles }} |
| Prefixes | {{fmtCount .Heaps.TotalPrefixes}} |
| Prefix Bytes | {{fmtBytes .Heaps.TotalPrefixBytes}} |

`))

var mdPrefixes = template.Must(tpl("prefixes").Parse(`
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

`))

const mdListUsersAndGroups = `
# Per User Reports - click on a link below
{{range $idx, $u := .Users}}{{if $idx}}, {{end}}[{{fmtUID .}}](#user-{{.}}){{end}}

# Per Group Reports - click on a link below
{{range $idx, $g := .Groups}}{{if $idx}}, {{end}}[{{fmtGID .}}](#group-{{.}}){{end}}  
`

var mdLists = template.Must(tpl("userGroupLists").Funcs(
	template.FuncMap{
		"fmtUID": globalUserManager.nameForUID,
		"fmtGID": globalUserManager.nameForGID,
	}).Parse(mdListUsersAndGroups))

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

var mdByUsers = template.Must(tpl("users").Funcs(template.FuncMap{"fmtID": globalUserManager.nameForUID}).Parse(mdByUsersGroupsTemplate))
var mdByGroups = template.Must(tpl("groups").Funcs(template.FuncMap{"fmtID": globalUserManager.nameForUID}).Parse(mdByUsersGroupsTemplate))

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

var mdPerUsers = template.Must(tpl("users").Funcs(
	template.FuncMap{"fmtID": globalUserManager.nameForUID}).
	Parse(mdPerUsersGroupsTemplate))
var mdPerGroups = template.Must(tpl("users").Funcs(
	template.FuncMap{"fmtID": globalUserManager.nameForGID}).
	Parse(mdPerUsersGroupsTemplate))

type mdHeap[T comparable] struct {
	TopN         int
	Prefix       string
	UserOrGroup  string
	Bytes        []reports.Zipped[T]
	StorageBytes []reports.Zipped[T]
	Files        []reports.Zipped[T]
	Prefixes     []reports.Zipped[T]
	PrefixBytes  []reports.Zipped[T]
}

type mdPerIDHeap struct {
	TopN        int
	Prefix      string
	UserOrGroup string
	PerID       map[uint32]mdHeap[string]
}

type markdownReports struct {
}

func (md *markdownReports) generateReports(ctx context.Context, rf *reportsFlags, prefix string, when time.Time, filenames *reportFilenames, data []byte) error {
	var sdb reports.AllStats
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&sdb); err != nil {
		return err
	}
	out := &bytes.Buffer{}

	// TOC & user/group links

	if err := mdTOC.Execute(out, struct {
		Prefix string
		TopN   int
		When   string
	}{
		Prefix: prefix,
		TopN:   rf.Markdown,
		When:   when.Format(time.RFC3339),
	}); err != nil {
		return err
	}

	uids, gids := maps.Keys(sdb.PerUser.ByPrefix), maps.Keys(sdb.PerGroup.ByPrefix)
	if err := mdLists.Execute(out, struct {
		Users, Groups []uint32
	}{
		Users:  uids,
		Groups: gids,
	}); err != nil {
		return err
	}

	// Totals.
	if err := mdTotals.Execute(out, struct {
		Prefix string
		Heaps  *reports.Heaps[string]
		When   string
	}{
		Prefix: prefix,
		Heaps:  sdb.Prefix,
		When:   when.Format(time.RFC3339),
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
	}

	if err := mdPrefixes.Execute(out, byPrefix); err != nil {
		return err
	}

	// Largest Users/Groups.
	byUsers := mdHeap[uint32]{
		Prefix:       prefix,
		TopN:         rf.Markdown,
		UserOrGroup:  "Users",
		Bytes:        reports.ZipN(sdb.ByUser.Bytes, rf.Markdown),
		Files:        reports.ZipN(sdb.ByUser.Files, rf.Markdown),
		Prefixes:     reports.ZipN(sdb.ByUser.Prefixes, rf.Markdown),
		StorageBytes: reports.ZipN(sdb.ByUser.StorageBytes, rf.Markdown),
		PrefixBytes:  reports.ZipN(sdb.ByUser.PrefixBytes, rf.Markdown),
	}

	if err := mdByUsers.Execute(out, byUsers); err != nil {
		return err
	}

	byGroups := mdHeap[uint32]{
		Prefix:       prefix,
		TopN:         rf.Markdown,
		UserOrGroup:  "Groups",
		Bytes:        reports.ZipN(sdb.ByGroup.Bytes, rf.Markdown),
		Files:        reports.ZipN(sdb.ByGroup.Files, rf.Markdown),
		Prefixes:     reports.ZipN(sdb.ByGroup.Prefixes, rf.Markdown),
		StorageBytes: reports.ZipN(sdb.ByGroup.StorageBytes, rf.Markdown),
		PrefixBytes:  reports.ZipN(sdb.ByGroup.PrefixBytes, rf.Markdown),
	}

	if err := mdByGroups.Execute(out, byGroups); err != nil {
		return err
	}

	for _, r := range []struct {
		label string
		tpl   *template.Template
		data  map[uint32]*reports.Heaps[string]
	}{
		{"user", mdPerUsers, sdb.PerUser.ByPrefix},
		{"group", mdPerGroups, sdb.PerGroup.ByPrefix},
	} {
		perUsers := mdPerIDHeap{
			TopN:        rf.Markdown,
			Prefix:      prefix,
			UserOrGroup: r.label,
			PerID:       make(map[uint32]mdHeap[string]),
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
	return os.WriteFile(filenames.summary("all"), out.Bytes(), 0666)
}
