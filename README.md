![linux](https://github.com/cloudengio/idu/actions/workflows/linux.yml/badge.svg)
![macos](https://github.com/cloudengio/idu/actions/workflows/macos.yml/badge.svg)
![windows](https://github.com/cloudengio/idu/actions/workflows/windows.yml/badge.svg)
![CodeQL](https://github.com/cloudengio/idu/actions/workflows/codeql.yml/badge.svg)

# idu - incremental, database backed, du.


`idu` analyzes file system structure and disk usage to build a database that
suports incremental re-scanning to support large local and clould based
fileystems. The analysis takes the form of scanning a filesystem, much like
du does, to gather information on file counts and sizes. `idu` is designed to
be extensible to cloud based filesystems as AWS' S3 or GCP's Cloud Storage.
It can report, from the database, aggregate statistics such as total
file counts, disk usage as well as 'top-n' statistics. It is also possible
to query the database in a variety of means, including a per-user basis.

Note that cloud based filesystems generally do not have a concrete
directory structure in the same way that local filesystems do.
S3 filenames for example have slash separated components 
but each component is not a directory in the sense that a user
can `cd` to it and list files relative to it. Instead, the separators
are purely a convention and S3 filenames can be accessed independently
of the slash separate components. For example, `s3:/aa/bb/cc` can be
listed as `aws s3 ls s3:/aa/b` or `aws s3 ls s3:/aa/bb/`. The former
will list all files starting with the prefix `/aa/b` whereas the
latter will only list files whose names start with `s3:/aa/bb`. Since
`idu` is intended to work with cloud based filesystems the term
`prefix` is often used instead of, or alongside, directory. Differences
in behaviour for different filesystems will be called out as they
are added. Currently only local filesystems are supported.

## Initial Configuration.

`idu` is configured using a yaml file, typically `$HOME/idu.yml`, but
this can overriden with `--config` file. This configuration file
has 3 main sections, `databases`, `layouts` and `exclusions`.

The `databases` section is mandatory and the others optional. A minimal
configuration is shown below and specifies the directory to store the
database in, the type of the database (in this `local` which uses
a simple local on-disk representation), and the file system prefix for
which this database will be used. In short, all files analyzed starting
with `/` will be recoreded in `$HOME/idu/all-local`.

```yaml
databases:
  - prefix: /
    type: local
    directory: $HOME/idu/all-local
```

Typically multiple databases will be used for distinct projects on shared
locally mounted filesystems, or for a local vs cloud hosted filesystem. It
is possible to nest databases so that a different database is used for `/tmp`
vs `/` but this should be used with care since the aggregate statistcs for `/`
will not include those from `/tmp` which may be confusing; in general
it is clearer to avoid nesting databases in this manner.

The additional sections of the configuration have defaults that allow them
to be omitted. `Layouts` are used to calculate disk usage by taking into
account file system block sizes, or more complex structures such as RAIDn.
The default is to assume a 4K block size as commonly used by most UNIX
systems. This default is equivalent to the following configuration
entry.

```yaml
layouts:
  - prefix: /
    type: block
    block_size: 4096
```

The `Exclusions` section can be used to exclude directories/prefixes
and/or files that match the supplied regular expression. For MacOS
systems for example it may be desirable to ignore the `.DS_Store` file,
which can be achieved as follows:

```yaml
exclusions:
  - prefix: /
    regexps:
      - ".DS_Store$"
```

The default is for no exclusions, ie. to include all files found.

# Common Use

Given a valid configuration file (shown below), `idu` can be used as outlined below.

```yaml
databases:
  - prefix: /projects/yourshared-project/
    type: local
    directory: /projects/yourshared-project/.idu/database
```

Common usage is as follows

```sh
$ idu analyze /projects/yourshared-project
$ idu errors /projects/yourshared-project
$ idu summary /projects/yourshared-project
$ idu lsr /projects/yourshared-project/a/b/c
$ idu find --prefix=testdata /projects/yourshared-project/a/b/c
$ idu find --file='/.*\.tar$' /projects/yourshared-project/a/b/c
```

As `idu` runs it will print various statistics that follow its progress. `idu`
may be safely interrupted and restarted (see [Incremental Updates]() below).

Once complete, it's good practice to see if `idu analyze` encountered any errors,
which are also written to the database, by running `idu errors` as show above. Note
that errors are common and most often due to permissions problems; `idu` records errors and leaves it to the user to decide whether they are relevant or not; for
example is a lot of disk usage behind an inaccessible due to permissions path?

 `idu summary` can be used to print summary statistics for the entire database,
 and lists the top-n files by size and directories/prefixes by the number of
 sub-directories or children they contain. The latter is useful for finding
 directories with large numbers of small files.

Statistics can also be generated dynamically from portions of the database
via the `lsr` command. It traverses the database and recomputes the statistics
for that portion only and can be used to drill into some subset of the files.

The `find` subcommand is intended for quickly finding directories/prefixes and files
that match a specific pattern or belong to a user or group. It prints an unsorted
list of names. The first example returns all prefixes that contain the `testdata`
anywhere in them (e.g. `/footestdata/a/b` will be returned as will `/a/testdata`);
use regular expression anchors (eg. `$`) and path separators to restrict the
search as required (e.g. `--prefix='/testdata$'` to find all trailing directories).
The `--file` regular expression is applied only the filename portion of.

Unlike the UNIX `find` command, `idu find` produces no output if a pattern is not specified. It is also differs in that `idu find` will match prefixes agains the
entire path, so patterns of the form `--prefix=/foo/bar` will match
`/a/foo/bar/baz`.

Note that the `lsr` command accepts options to restrict its output and statistics
calculations to files for a specific user (`--user`).

## Common Pitfalls

Be sure to quote arguments to `idu` in case they contain spaces.

```sh
$ idu analyze "$HOME"
```

`idu` does not follow UNIX soft-links; in this situations, a trailing / can
be used to have the shell follow the softink. For example if `"$HOME/Dropbox"`
is a soft link, then use `"$HOME/Dropbox/"`

```sh
$ idu analyze "$HOME/Dropbox/"
```

The trailing / should also be included in the `Prefix:` keys in the
configuration file.

## Per User Statistics

The `user` command can be used to display statistics for a particular user
within a prefix. The command below will calculate statistics for the user
`joe` within the shared project area.

```sh
idu user /projects/yourshared-project joe
```

It's also possible to list all users and to display their statistics.

```sh
idu user --list-users /projects/yourshared-project
idu user --all-users /projects/yourshared-project
```

The `idu user` subcommand can also write the per-user summaries to text
files in the specified directory.

```sh
idu user --all-users --user-reports-dir=user-reports /projects/yourshared-project
```

Finally, the `lsr` subcommand can filter for a single user as follows:

```sh
idu lsr --user=joe /projects/yourshared-project/a/subtree/of/interest
```

## Incremental Updates.

Once an initial analysis run is complete and the database initialized
incremental updates are possible. If the target filesystem contains
immutable files (ie. their sizes do not change in place) then `idu` can
safely skip directories/prefixes which have not themselves changed. However,
if filesizes are changing `idu` must re-read the metadata for each file; instead,
a portion of the original filesystem can be re-scanned to incrementally update
the database.

For example, running idu multiple times on a `Downloads` directory,
which typically contains immutable, files will use the database to avoid
rescanning directories that have not changed.

```sh
$ idu anaylyze $HOME/Downloads
$ idu anaylyze $HOME/Downloads
```

If file sizes are changing, then the second `idu` invocation below
will update the database, and its statistics, with the new information
from `dir1/dir2` without having to re-analyze the entire home directory.

```sh
$ idu anaylyze $HOME
$ idu analyyze $HOME/dir1/dir2
```

## Anticipated Changes and Improvements

### Cloud
`idu` was designed with cloud filesystems and support for GCP's Cloud
Storage and AWS S3 will be added in the near future.

### Query Support

Some form of query facility, against an index of filename components
as well as complete filenames would be a useful addition. Especially
if queries can be used to define the input and output files for particular
tasks. Computing metrics across those files, such size distributions is
also anticipated. The results of queries could be logged to the database
to allow for tracking changes to the results over time to catch
situations where data is added, removed or changed in place.

### Annotations and Links to External Documentation/Data

In many cases the raw data in a filesystem in one location is associated
with metadata stored elsewhere in the same filesystem or some other system.
Providing a means of annotating the filesystem with such links or simple
text annotations would help manage these associations.

## Multiple FileSystems and Databases.

`idu` configuration and operation allow for analyzing multiple
filesystems and associating a database, layouts and exlusions
with each such filesystem. Currently each database is treated independently,
but operations such as querying and annotating will be extended to
work across multiple databases and filesystems making it easily search all
filesystems simultaneously (and concurrently).
