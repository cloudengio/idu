![linux](https://github.com/cloudengio/idu/actions/workflows/linux.yml/badge.svg)
![macos](https://github.com/cloudengio/idu/actions/workflows/macos.yml/badge.svg)
![windows](https://github.com/cloudengio/idu/actions/workflows/windows.yml/badge.svg)
![CodeQL](https://github.com/cloudengio/idu/actions/workflows/codeql.yml/badge.svg)

# idu - incremental, database backed, du.

`idu` analyzes a file system to build a database that
supports incremental re-scanning to handle large local and cloud-based
filesystems. The analysis takes the form of scanning a filesystem, much like
`du` does, to gather information on file counts and sizes. An important
difference to `du` is that `idu` is heavily optimized for concurrent execution
and can easily handle issuing 1000s of simultaneous stat requests and
directory scans. For example it can scan an Apple Silicon MacBook in
around 10 minutes and a 14M+ file Lustre filesystem in around 50 minutes.
`idu` is designed to be extensible to cloud-based filesystems such as AWS S3
or GCP Cloud Storage though this is not yet implemented.
It can report, from the database, aggregate statistics such as total
file counts, disk usage and generate reports in json and markdown formats.
It is also possible to query the database in a variety of ways,
including on a per-user basis.

Note that cloud-based filesystems generally do not have a concrete
directory structure in the same way that local filesystems do.
S3 filenames for example have slash-separated components,
but each component is not a directory in the sense that a user
can `cd` to it and list files relative to it. Instead, the separators
are purely a convention and S3 filenames can be accessed independently
of the slash-separated components. For example, `s3:/aa/bb/cc` can be
listed as `aws s3 ls s3:/aa/b` or `aws s3 ls s3:/aa/bb/`. The former
will list all files starting with the prefix `/aa/b` whereas the
latter will only list files whose names start with `s3:/aa/bb`. Since
`idu` is intended to work with cloud-based filesystems the term
`prefix` is often used instead of, or along with, directory. Differences
in behaviour for different filesystems will be called out as they
are added. Currently only local filesystems are supported.

## Configuration

`idu` loads YAML (`.yaml`, `.yml`) and optional `.env` configuration files
using the `idu` configuration search path:

1. `.` (current working directory)
2. `os.UserConfigDir()/idu` (for example `$HOME/Library/Application Support/idu` on macOS)
3. `$HOME/.config/idu`

When `--config` and `IDU_CONFIG` are not set:

- If `config.yaml` (or `config.yml`) exists in the search path, `idu` reads it
  and searches for any additional files listed under its top-level `filesystem_config_files:`
  key, merging all `filesystems:` entries in order. Missing entries in
  `filesystem_config_files:` are reported as errors along with the files read.
- Otherwise, `idu` discovers and merges all `.yaml`/`.yml` and `.env` files in
  the user config directories (`os.UserConfigDir()/idu` and `$HOME/.config/idu`).

You can also pass explicit files via `--config=a.yaml,b.yaml` (or repeat
`--config`) or set `IDU_CONFIG` / `IDU_ENV`. Loaded files support strict field
validation, `vars:` YAML variables, `.env`/environment `${VAR}` expansion,
YAML anchors (`&anchor`) and sequence merges (`- <<: *anchor`).

Each configuration has top-level keys `filesystems:` and optional `filesystem_config_files:`.
A minimal `$HOME/.config/idu/config.yaml` looks like:

```yaml
vars:
  db_root: /var/db/idu

filesystem_config_files:
  - home.yaml
  - lustre.yaml

filesystems:
  - name: tmp
    root: /tmp
    database: ${db_root}/tmp
```

And a per-filesystem file `$HOME/.config/idu/home.yaml` (either a list of
filesystem entries or a mapping with `filesystems:`):

```yaml
- name: home
  root: /my/home/tree
  database: ${db_root}/home
```

Each filesystem entry specifies either a `root:` for a hierarchical
filesystem (`prefix:` is accepted as a deprecated alias for `root:`), or a
range of keys in a key oriented store, such as an object store, using
`start_prefix:` and, optionally, `stop_prefix:`. The range includes
`start_prefix` and excludes `stop_prefix`, which is the first key, in
lexicographic order, outside of the range. If `stop_prefix` is omitted the
range contains exactly those keys that start with `start_prefix`:

```yaml
- name: alpha-objects
  start_prefix: projects/alpha/
  database: ${db_root}/alpha-objects
```

For a root, a `<prefix>` argument is a path relative to, or within, the root;
for a key range it is a key prefix whose keys must all lie within the range.
Note that `analyze` does not yet support key ranges.

Common concurrency and batching options per filesystem entry:

```yaml
    concurrent_scans: 5000 # scan up to 5000 directories concurrently.
    concurrent_stats: 2000 # issue at most 2000 concurrent stat operations.
    concurrent_stats_threshold: 10 # issue asynchronous stats if the number of files in a directory exceeds 10.
    scan_size: 2000 # scan 2000 items at a time from each directory.
```

`exclusions` can be used to ignore directories/prefixes and/or files that match
the supplied regular expressions. On macOS, for example:

```yaml
    exclusions:
      - '.DS_Store$'
      - '^/Users/someone/Library/CloudStorage'
```

It is also possible to specify the file system separator (`/` for Unix, `\` for Windows):

```yaml
    separator: \
```

The `layout` section calculates raw disk usage by accounting for filesystem
block sizes (`block`), `bytes` (identity), or `raid0` parameters. Byte sizes
accept either bare integers or unit strings (`4KiB`, `64KiB`, `1MB`, etc.):

```yaml
    layout:
      calculator: block
      parameters:
        size: 4KiB
```

Use `idu config describe` to display all supported configuration options and
`idu config show` to inspect the merged configuration and loaded files.

See [example-config.yml](example-config.yml) for a complete annotated example.

## Common Use

Given a valid configuration file:

```yaml
filesystems:
  - name: shared-project
    root: /projects/yourshared-project/
    database: /projects/yourshared-project/.idu/database
```

The filesystem to operate on is selected by its configured `name` using the
global `--fs` flag; `--fs` may be omitted when only one filesystem is
configured. Any command taking a `<prefix>` interprets it relative to the
filesystem's configured root (an absolute path may also be used) and it must
be contained within that root. Commands that take an optional `[<prefix>]`
default to the filesystem's root:

```sh
$ idu --fs=shared-project analyze
$ idu --fs=shared-project analyze some/subdir
$ idu --fs=shared-project errors
$ idu --fs=shared-project analyze --snapshot
$ idu --fs=shared-project stats view
$ idu --fs=shared-project reports generate
$ idu --fs=shared-project stats compute some/subdir 'user=alice'
```

You can also inspect the current configuration and database location:

```sh
$ idu config show
$ idu config describe
$ idu --fs=shared-project database locate
```

As `idu` runs it will print statistics that follow its progress. `idu` may be
safely interrupted and restarted for incremental updates.

Once complete, it's good practice to see if `idu analyze` encountered any errors,
which are also written to the database, by running `idu errors` as shown above. Note
that errors are common and most often due to permission problems; `idu` records
errors and leaves it to the user to decide whether they are relevant (for example,
whether significant disk usage sits behind an inaccessible path).

`stats view` displays the statistics for the entire filesystem and
`reports generate` writes reports (Markdown, JSON, TSV) for them. Both use the
most recent snapshot in the filesystem's `snapshot_dir` if it is more recent
than the last update to the database, and otherwise construct the statistics
from the rankings and totals that `analyze` maintains in the database. When
those cannot be used, for example after an interrupted analysis, the
statistics require a full pass over the database and are saved as a new
snapshot, if `snapshot_dir` is set, so that the pass is not repeated.

Snapshots are stored in `snapshot_dir`, each named by the time it was
created, with a symlink `latest` to the most recent. `analyze --snapshot`
writes one once the analysis has completed, which requires `snapshot_dir` to
be set. `stats compute <prefix> [<expression>...]` computes statistics for any
prefix, optionally restricted by an expression, and saves them as a snapshot
in `--snapshot-dir` or `snapshot_dir` (or to `--stats-file`). A snapshot can be
viewed, or reported on, by naming it: `stats view <snapshot>` and
`reports generate <snapshot>...`.

Reports are written to `--report-dir`, or the filesystem's `reports_dir`,
defaulting to `reports`, each set in a directory named by the time of the
statistics it was generated from, with a symlink `latest` to the most recent.
`reports locate` lists the most recent sets in the same directory.

`stats view` displays everything other than the hardlinks section by default;
`--summary`, `--bytes`, `--storage`, `--file-counts`, `--hotspots` and
`--hardlinks` select what is displayed, and may be combined. `--summary`
displays the totals, `--bytes`, `--storage` and `--file-counts` the top
entries by reported bytes, bytes stored and number of files respectively,
`--hotspots` the distributed subtree hotspots, ie. subtrees whose usage is at
least twice that of their largest directory, and `--hardlinks` the number of
hardlinks, the top entries by number of hardlinks and any files found by
analyze to have links that it has not seen. The hotspots and the hardlink
rankings are not part of the state maintained by analyze and hence require a
full pass over the database, which is saved as a snapshot, unless the current
snapshot already includes them.

Per-user or per-group statistics can be viewed as follows, subset by the same
flags:

```sh
$ idu stats view --user=<user> [<snapshot>]
$ idu stats view --group=<group> [<snapshot>]
```

## Anticipated Changes and Improvements

### Cloud
`idu` was designed with cloud filesystems in mind and support for GCP's Cloud
Storage and AWS S3 will be added in the future.
