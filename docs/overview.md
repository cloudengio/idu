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
    prefix: /tmp
    database: ${db_root}/tmp
```

And a per-filesystem file `$HOME/.config/idu/home.yaml` (either a list of
filesystem entries or a mapping with `filesystems:`):

```yaml
- name: home
  prefix: /my/home/tree
  database: ${db_root}/home
```

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
    prefix: /projects/yourshared-project/
    database: /projects/yourshared-project/.idu/database
```

Any command taking `<prefix>` accepts either the filesystem prefix path or its
configured `name`:

```sh
$ idu analyze shared-project
$ idu errors shared-project
$ idu stats compute --stats-dir=./stats /projects/yourshared-project/
$ idu stats view ./stats/latest.idustats
$ idu reports generate ./stats/latest.idustats
```

You can also inspect the current configuration and database location:

```sh
$ idu config show
$ idu config describe
$ idu database locate shared-project
```

As `idu` runs it will print statistics that follow its progress. `idu` may be
safely interrupted and restarted for incremental updates.

Once complete, it's good practice to see if `idu analyze` encountered any errors,
which are also written to the database, by running `idu errors` as shown above. Note
that errors are common and most often due to permission problems; `idu` records
errors and leaves it to the user to decide whether they are relevant (for example,
whether significant disk usage sits behind an inaccessible path).

`stats compute <prefix>` computes stats from the database and stores them in a
timestamped file in `--stats-dir` (or `stats_dir` from the filesystem's
configuration, defaulting to `stats`, and creates a symlink `latest.idustats` to
the file produced). `stats view <idustats-file>` reads stats and prints them to
stdout. `reports generate <idustats-file>` generates reports (Markdown, JSON,
TSV).

Per-user or per-group statistics can be viewed as follows:

```sh
$ idu stats view --user=<user> <idustats-file>
$ idu stats view --group=<group> <idustats-file>
```

## Anticipated Changes and Improvements

### Cloud
`idu` was designed with cloud filesystems in mind and support for GCP's Cloud
Storage and AWS S3 will be added in the future.
