# Package [cloudeng.io/cmd/idu/internal](https://pkg.go.dev/cloudeng.io/cmd/idu/internal?tab=doc)

```go
import cloudeng.io/cmd/idu/internal
```


## Functions
### Func SubtreeMatch
```go
func SubtreeMatch(key, target, sep string) (match, stop bool)
```
SubtreeMatch reports whether key equals target or is beneath target
delimited by sep, and whether lexicographical scan has passed target's
subtree. Sibling keys sorting between target and target+sep (such as /a-b
for /a) return (false, false) so scanning skips them and continues to /a/...
descendants.




