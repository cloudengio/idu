![linux](https://github.com/cloudengio/idu/actions/workflows/linux.yml/badge.svg)
![macos](https://github.com/cloudengio/idu/actions/workflows/macos.yml/badge.svg)
![windows](https://github.com/cloudengio/idu/actions/workflows/windows.yml/badge.svg)
![CodeQL](https://github.com/cloudengio/idu/actions/workflows/codeql.yml/badge.svg)

# idu - incremental, database backed, du.

`idu` analyzes a file system to build a database that
suports incremental re-scanning to support large local and clould based
fileystems. The analysis takes the form of scanning a filesystem, much like
du does, to gather information on file counts and sizes. An important
difference to du is that idu is heavily optimized for concurrent execution
and can easily handle issuing 1000s of simulataneous stat requests and
directory scans. For example it can scan an Apple Silicon macbook in
around 10 minutes and a 14M+ file lustre filesystem in around 50 minutes.
`idu` is designed to be extensible to cloud based filesystems as AWS' S3
or GCP's Cloud Storage though this is not yet implemented.
It can report, from the database, aggregate statistics such as total
file counts, disk usage and to generate reports in json, markdown formats.
It is also possible to query the database in a variety of means,
including a per-user basis.

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
`prefix` is often used instead of, or along with, directory. Differences
in behaviour for different filesystems will be called out as they
are added. Currently only local filesystems are supported.

## Configuration.

`idu` is configured using a yaml file, typically `$HOME/idu.yml`, but
this can overriden with `--config` file. This configuration file
is organized as a list of 'prefix' entries, each of which
specifies a filesystem tree to be used with `idu`.

Each prefix entry specifies the tree to be scanned, the location of the
database to be used/created and various, optional, configuration parameters.
A minimal entry is as follows:
```yaml
- prefix: /my/home/tree
  database: /my/home/database/location
```

Common options control the degree of concurrency to use when analyzing
a prefix. These are:

```yaml
  concurrent_scans: 5000 # scan up to 5000 directories concurrently.
  concurrent_stats: 2000 # issue at most 2000 concurrent stat operations.
  concurrent_stats_threshold: 10 # issue asynchronous stats if the number of files in a directory exceeds 10.
  scan_size: 2000 # scan 2000 items at a time from each directory
```

Additional options are available to specify exclusions and file system
specific otions.

`Exclusions` section can be used to exclude directories/prefixes
and/or files that match the supplied regular expression. For MacOS
systems for example it may be desirable to ignore the `.DS_Store` file,
and the `CloudStorage` directory, which can be achieved as follows:

```yaml
  exclusions:
  - '.DS_Store$'
  - '^/User/someone/Library/CloudStorage'
```

It is possible to specify the file system separator (/ for Unix, \ for windows).
```yaml
  separator: \
```

The `layouts` section is used to calculate disk usage by taking into
account file system block sizes, or more complex structures such as RAID.

```yaml
  layout:
    type: block
    block_size: 4096
```

# Common Use

Given a valid configuration file (shown below), `idu` can be used as outlined below.

```yaml
- prefix: /projects/yourshared-project/
  database: /projects/yourshared-project/.idu/database
```

Common usage is as follows:

```sh
$ idu analyze /projects/yourshared-project/
$ idu errors /projects/yourshared-project/
$ idu stats compute /projects/yourshared-project/
$ idu stats aggregate /projects/yourshared-project/
$ idu reports generate /projects/yourshared-project/
```

As `idu` runs it will print various statistics that follow its progress. `idu`
may be safely interrupted and restarted (see [Incremental Updates]() below).

Once complete, it's good practice to see if `idu analyze` encountered any errors,
which are also written to the database, by running `idu errors` as show above. Note
that errors are common and most often due to permissions problems; `idu` records errors and leaves it to the user to decide whether they are relevant or not; for
example is a lot of disk usage behind an inaccessible due to permissions path?

```stats compute <prefix>``` will compute stats for the entire database and store
them in the database. ```stats aggregate <prefix>``` can be used to
read the stats from the database, or to recompute stats for a portion of the database.

```sh
$ idu stats compute /projects/yourshared-project/  # stores the stats in the database
$ idu stats aggregate /projects/yourshared-project/ # reads the stats from the database
$ idu stats aggregate /projects/yourshared-project/a/subtree/of/interest # recompute stats for a portion of the database
```

Per-user or per-group statistics can be computed as follows:

```sh
$ idu stats user /projects/yourshared-project/ <user>...
$ idu stats group /projects/yourshared-project/ <group>...
```


## Anticipated Changes and Improvements

### Cloud
`idu` was designed with cloud filesystems and support for GCP's Cloud
Storage and AWS S3 will be added in the near future.


