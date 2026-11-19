// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"maps"
	"os"
	"regexp"
	"slices"
	"strings"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/summary"
)

type suggestFlags struct {
	N      int  `subcmd:"n,20,maximum number of prefixes with missing links to consider"`
	Locate bool `subcmd:"locate,true,'scan the metadata for the entire filesystem to locate the prefixes that contain the other links to the files with missing links, which are the prefixes to add to hardlinks_record_all'"`
}

func (st *statsCmds) suggest(ctx context.Context, values interface{}, args []string) error {
	return st.suggestTo(ctx, os.Stdout, values.(*suggestFlags), args)
}

// linkOrigin identifies a file with missing links.
type linkOrigin struct {
	dev, ino uint64
}

// staleLinks describes the files in a prefix whose stored link count is
// stale, as found by suggest.
type staleLinks struct {
	files int
	from  map[string]bool // the prefixes with missing links
}

func (st *statsCmds) suggestTo(ctx context.Context, out io.Writer, sf *suggestFlags, args []string) error {
	ctx, f, target, err := lookupPrefix(ctx, globalConfig, prefixArg(args))
	if err != nil {
		return err
	}
	cfg := f.Config()
	db, err := openReadOnlyDB(ctx, cfg)
	if err != nil {
		return fmt.Errorf("failed to open database for %v in %v: %w", iduconfig.Label(f), cfg.Database, err)
	}
	defer db.Close(ctx)

	fmt.Fprintf(out, "# Suggested settings for %v.\n#\n", iduconfig.Label(f))
	if !cfg.RecordHardlinks() || cfg.TopNOrDefault() == 0 {
		fmt.Fprintf(out, "# Missing links are only detected when hardlink_dedup is summaries or\n# summaries-all and topn is not zero.\n")
		return nil
	}
	ml, err := idudb.ReadMissingLinks(ctx, db)
	if err != nil {
		return err
	}
	sep := cfg.SeparatorOrDefault()
	var missing []idudb.MissingLinksPrefix
	for _, p := range ml.Prefixes {
		if inSubtree, _ := internal.SubtreeMatch(p.Prefix, target, sep); inSubtree {
			missing = append(missing, p)
		}
	}
	if len(missing) == 0 {
		fmt.Fprintf(out, "# analyze found no files with missing links.\n")
		return nil
	}
	slices.SortStableFunc(missing, func(a, b idudb.MissingLinksPrefix) int {
		return cmp.Compare(b.Links, a.Links)
	})
	if sf.N > 0 && len(missing) > sf.N {
		missing = missing[:sf.N]
	}
	fmt.Fprintf(out, "# analyze found files with more links than it has seen. Their other\n"+
		"# links are outside of the filesystem, are excluded, or are in prefixes\n"+
		"# that have not been rescanned since the links were created and hence\n"+
		"# whose stored link counts are stale. Only the last of these causes the\n"+
		"# files to be counted more than once, which is prevented by adding the\n"+
		"# prefixes that contain them to hardlinks_record_all.\n#\n")
	if !sf.Locate {
		printMissing(out, "The prefixes containing the files with missing links are:", missing)
		fmt.Fprintf(out, "#\n# Omit --locate=false to find the prefixes that contain their other links.\n")
		return nil
	}
	stale, err := locateStaleLinks(ctx, summary.New(db, cfg), cfg, f.Location(), missing)
	if err != nil {
		return err
	}
	printSuggestions(out, stale, missing)
	return nil
}

// locateStaleLinks scans the metadata for the entire filesystem, rooted at
// root, for the prefixes that contain other links, with a stale link count
// of one, to the files with missing links.
func locateStaleLinks(ctx context.Context, sr *summary.Reader, cfg *iduconfig.Settings, root string, missing []idudb.MissingLinksPrefix) (map[string]*staleLinks, error) {
	origins, err := linkOrigins(ctx, sr, missing)
	if err != nil {
		return nil, err
	}
	stale := map[string]*staleLinks{}
	if len(origins) == 0 {
		return stale, nil
	}
	for p, err := range sr.Prefixes(ctx, root) {
		if err != nil {
			return nil, err
		}
		if cfg.RecordAllFilesFor(p.Path()) {
			continue
		}
		for _, e := range p.Entries() {
			if e.IsDir() || e.XAttr.Hardlinks != 1 {
				continue
			}
			froms := origins[linkOrigin{dev: e.XAttr.Device, ino: e.XAttr.FileID}]
			matched := false
			for _, from := range froms {
				if from == p.Path() {
					continue
				}
				if !matched {
					if stale[p.Path()] == nil {
						stale[p.Path()] = &staleLinks{from: map[string]bool{}}
					}
					stale[p.Path()].files++
					matched = true
				}
				stale[p.Path()].from[from] = true
			}
		}
	}
	return stale, nil
}

// linkOrigins returns the prefixes that contain each of the files that may
// be hardlinks in the prefixes with missing links.
func linkOrigins(ctx context.Context, sr *summary.Reader, missing []idudb.MissingLinksPrefix) (map[linkOrigin][]string, error) {
	origins := map[linkOrigin][]string{}
	for _, m := range missing {
		p, ok, err := sr.Prefix(ctx, m.Prefix)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		for _, e := range p.Entries() {
			if !e.IsDir() && e.XAttr.FileID != 0 && e.XAttr.Hardlinks > 1 {
				in := linkOrigin{dev: e.XAttr.Device, ino: e.XAttr.FileID}
				if !slices.Contains(origins[in], m.Prefix) {
					origins[in] = append(origins[in], m.Prefix)
				}
			}
		}
	}
	return origins, nil
}

func printSuggestions(out io.Writer, stale map[string]*staleLinks, missing []idudb.MissingLinksPrefix) {
	if len(stale) == 0 {
		fmt.Fprintf(out, "# No prefixes with stale link counts were found, the other links are\n"+
			"# outside of the filesystem or are excluded and hence no files are\n"+
			"# counted more than once.\n#\n")
		printMissing(out, "The prefixes containing the files with missing links are:", missing)
		return
	}
	fmt.Fprintf(out, "# The following prefixes contain links whose stored link counts are\n"+
		"# stale. Add them, or patterns that match them such as one for a common\n"+
		"# ancestor, to hardlinks_record_all for this filesystem; this causes\n"+
		"# the next analyze to rewrite all of the summaries.\n")
	fmt.Fprintf(out, "hardlinks_record_all:\n")
	for _, p := range slices.Sorted(maps.Keys(stale)) {
		s := stale[p]
		fmt.Fprintf(out, "  # %v %v linked from %v\n", s.files, plural(s.files, "file", "files"), strings.Join(slices.Sorted(maps.Keys(s.from)), ", "))
		fmt.Fprintf(out, "  - %v\n", yamlQuote("^"+regexp.QuoteMeta(p)+"$"))
	}
	located := map[string]bool{}
	for _, s := range stale {
		for from := range s.from {
			located[from] = true
		}
	}
	var others []idudb.MissingLinksPrefix
	for _, m := range missing {
		if !located[m.Prefix] {
			others = append(others, m)
		}
	}
	if len(others) > 0 {
		fmt.Fprintf(out, "#\n")
		printMissing(out, "No stale links were found for the files with missing links in:", others)
	}
}

func printMissing(out io.Writer, banner string, missing []idudb.MissingLinksPrefix) {
	fmt.Fprintf(out, "# %v\n", banner)
	for _, m := range missing {
		fmt.Fprintf(out, "#   %v: %v %v, %v missing %v", m.Prefix, m.Files, plural(m.Files, "file", "files"), m.Links, plural(int(min(m.Links, 2)), "link", "links")) //nolint:gosec
		if len(m.Examples) > 0 {
			fmt.Fprintf(out, ", eg. %v", strings.Join(m.Examples, ", "))
		}
		fmt.Fprintf(out, " (%v)\n", m.Updated.Format("2006-01-02 15:04:05"))
	}
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// yamlQuote returns s as a single-quoted YAML scalar.
func yamlQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
