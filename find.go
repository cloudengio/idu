// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

/*
type findFlags struct {
	User        string          `subcmd:"user,,restrict output to the specified user"`
	Group       string          `subcmd:"group,,restrict output to the specified group"`
	PrefixMatch flags.Repeating `subcmd:"prefix,,a regular expression to match against prefix/directory names against"`
	FileMatch   flags.Repeating `subcmd:"file,,a regular expression to match against filenames against"`
	ShowSizes   bool            `subcmd:"sizes,true,'show usage, number of files, children etc'"`
	Sort        bool            `subcmd:"sort,false,'sort found files by diskusage, file and child count'"`
	TopN        int             `subcmd:"top,100,'show the top prefixes by file/prefix counts and disk usage'"`
}

type finder struct {
	pt               *progressTracker
	db               internal.Database
	sep              string
	user, group      string
	prefixRE, fileRE []*regexp.Regexp
}

type results struct {
	prefix            string
	sep               string
	prefixInfo        internal.PrefixInfo
	nFiles, nChildren int
}

func match(regexps []*regexp.Regexp, value string) bool {
	for _, r := range regexps {
		if r.MatchString(value) {
			return true
		}
	}
	return false
}

func (fr *finder) find(ctx context.Context, resultsCh chan results, root string) error {
	sc := fr.db.NewScanner(root, 0, internal.ScanLimit(100000))
	user, group := fr.user, fr.group
	prefixRE, fileRE := fr.prefixRE, fr.fileRE
	for sc.Scan(ctx) {
		prefix, pi := sc.PrefixInfo()
		found := *pi
		found.Children = nil
		found.Files = nil
		result := results{
			prefix:     prefix,
			sep:        fr.sep,
			prefixInfo: found,
			nFiles:     len(pi.Files),
			nChildren:  len(pi.Children),
		}
		if len(user) > 0 && pi.UserID == user {
			resultsCh <- result
			continue
		}
		if len(group) > 0 && pi.GroupID == group {
			resultsCh <- result
			continue
		}
		if prefixRE != nil {
			if match(prefixRE, prefix) {
				resultsCh <- result
			}
		}
		if fileRE == nil {
			continue
		}
		for _, fi := range pi.Files {
			if match(fileRE, fi.Name()) {
				found.Files = append(found.Files, fi)
			}
		}
		if len(found.Files) > 0 {
			resultsCh <- results{prefix: prefix, prefixInfo: found}
		}
	}
	return sc.Err()
}

func compileRE(arg string, expressions flags.Repeating) ([]*regexp.Regexp, error) {
	if len(expressions.Values) == 0 {
		return nil, nil
	}
	res := make([]*regexp.Regexp, len(expressions.Values))
	for i, v := range expressions.Values {
		r, err := regexp.Compile(v)
		if err != nil {
			return nil, fmt.Errorf("failed to compile regexp for --%v: %v: %v", arg, v, err)
		}
		res[i] = r
	}
	return res, nil
}

func find(ctx context.Context, values interface{}, args []string) error {
	flagValues := values.(*findFlags)

	userKey := ""
	if usr := flagValues.User; len(usr) > 0 {
		userKey = globalUserManager.uidForName(usr)
	}
	groupKey := ""
	if grp := flagValues.Group; len(grp) > 0 {
		groupKey = globalUserManager.gidForName(grp)
	}
	errs := errors.M{}
	prefixRE, err := compileRE("prefix", flagValues.PrefixMatch)
	errs.Append(err)
	fileRE, err := compileRE("file", flagValues.FileMatch)
	errs.Append(err)
	if err := errs.Err(); err != nil {
		return err
	}

	resultsCh := make(chan results, 1000)
	pt := newProgressTracker(ctx, time.Second)
	finders := &errgroup.T{}
	finders = errgroup.WithConcurrency(finders, len(args))
	for _, root := range args {
		root := root
		db, err := globalDatabaseManager.DatabaseFor(ctx, root, internal.ReadOnly())
		if err != nil {
			return err
		}
		layout := globalConfig.LayoutFor(root)
		f := &finder{
			pt:       pt,
			db:       db,
			sep:      layout.Separator,
			user:     userKey,
			group:    groupKey,
			prefixRE: prefixRE,
			fileRE:   fileRE,
		}
		finders.Go(func() error {
			return f.find(ctx, resultsCh, root)
		})
	}

	go func() {
		errs.Append(finders.Wait())
		close(resultsCh)
	}()

	files, children, disk := heap.NewKeyedInt64(heap.Descending), heap.NewKeyedInt64(heap.Descending), heap.NewKeyedInt64(heap.Descending)
	ifmt := message.NewPrinter(language.English)
	for result := range resultsCh {
		pi := result.prefixInfo
		if flagValues.Sort {
			files.Update(result.prefix, int64(result.nFiles))
			children.Update(result.prefix, int64(result.nChildren))
			disk.Update(result.prefix, pi.DiskUsage)
			continue
		}
		ifmt.Printf("%v", result.prefix)
		if flagValues.ShowSizes {
			ifmt.Printf(" %v", fsize(pi.DiskUsage))
			if result.nChildren > 0 || result.nFiles > 0 {
				ifmt.Printf(" (children/files):  (%v/%v)",
					result.nChildren, result.nFiles)
			}
		}
		ifmt.Printf("\n")
		if len(pi.Files) == 0 {
			continue
		}
		prefix := strings.TrimSuffix(result.prefix, result.sep)
		for _, fi := range pi.Files {
			if flagValues.ShowSizes {
				ifmt.Printf("%v%s%v: %v\n", prefix, result.sep, fi.Name, fsize(pi.Size))
			} else {
				ifmt.Printf("%v%s%v\n", prefix, result.sep, fi.Name)
			}
		}
	}
	errs.Append(globalDatabaseManager.CloseAll(ctx))
	if flagValues.Sort {
		nFiles, nChildren, nBytes := files.Sum(), children.Sum(), disk.Sum()
		topFiles, topChildren, topBytes := topNMetrics(files.TopN(flagValues.TopN)),
			topNMetrics(children.TopN(flagValues.TopN)),
			topNMetrics(disk.TopN(flagValues.TopN))

		printSummaryStats(ctx, os.Stdout, nFiles, nChildren, nBytes, 0, flagValues.TopN, topFiles, topChildren, topBytes)
	}
	return errs.Err()
}
*/
