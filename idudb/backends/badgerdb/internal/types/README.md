# Package [cloudeng.io/cmd/idu/idudb/backends/badgerdb/internal/types](https://pkg.go.dev/cloudeng.io/cmd/idu/idudb/backends/badgerdb/internal/types?tab=doc)

```go
import cloudeng.io/cmd/idu/idudb/backends/badgerdb/internal/types
```


## Functions
### Func Decode
```go
func Decode[T any](buf []byte, v *T) error
```

### Func Encode
```go
func Encode[T any](buf *bytes.Buffer, v T) error
```



## Types
### Type ErrorPayload
```go
type ErrorPayload struct {
	When    time.Time
	Key     string
	Payload []byte
}
```


### Type LogPayload
```go
type LogPayload struct {
	Start, Stop time.Time
	Payload     []byte
}
```


### Type StatsPayload
```go
type StatsPayload struct {
	When    time.Time
	Payload []byte
}
```





