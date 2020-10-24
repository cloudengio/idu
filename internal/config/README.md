# Package [cloudeng.io/cmd/idu/internal/config](https://pkg.go.dev/cloudeng.io/cmd/idu/internal/config?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/internal/config)](https://goreportcard.com/report/cloudeng.io/cmd/idu/internal/config)

```go
import cloudeng.io/cmd/idu/internal/config
```


## Functions
### Func Documentation
```go
func Documentation() string
```
Documentation will return a description of the format of the yaml
configuration file.



## Types
### Type Config
```go
type Config struct {
	Databases  []Database   // Per-prefix databases.
	Layouts    []Layout     // Per-prefix layouts.
	Exclusions []Exclusions // Per-prefix exclusions.
}
```
Config represents a complete configuration.

### Functions

```go
func ParseConfig(buf []byte) (*Config, error)
```
ParseConfig will parse a yaml config from the supplied byte slice.


```go
func ReadConfig(filename string) (*Config, error)
```
ReadConfig will read a yaml config from the specified file.



### Methods

```go
func (cfg *Config) DatabaseFor(prefix string) (Database, bool)
```


```go
func (cfg *Config) ExclusionsFor(prefix string) (Exclusions, bool)
```


```go
func (cfg *Config) LayoutFor(prefix string) Layout
```




### Type Database
```go
type Database struct {
	Prefix      string
	Type        string
	Open        DatabaseOpenFunc
	Delete      DatabaseDeleteFunc
	Description string
}
```
Database represents a means of creating instances of filewalk.Database
configured as per the yaml configuration file that is to be used for storing
data for entries within the specified prefix.


### Type DatabaseDeleteFunc
```go
type DatabaseDeleteFunc func(ctx context.Context) error
```
DatabaseDeleteFunc is called to delete an instance of filewalk.Database.


### Type DatabaseOpenFunc
```go
type DatabaseOpenFunc func(ctx context.Context, opts ...filewalk.DatabaseOption) (filewalk.Database, error)
```
DatabaseOpenFunc is called to open a filewalk.Database instance in write or
read-only mode.


### Type Exclusions
```go
type Exclusions struct {
	Prefix  string
	Regexps []*regexp.Regexp
}
```
Exclusions represents a set of exclusion regular expressions to apply to a
prefix.


### Type Layout
```go
type Layout struct {
	Prefix     string
	Separator  string
	Calculator diskusage.Calculator
}
```
Layout represents a means of calculating the disk usage for files with the
specified prefix.





