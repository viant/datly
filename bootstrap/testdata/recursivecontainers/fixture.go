package recursivecontainers

import "reflect"

type Recursive map[string]Recursive

var Anchor = reflect.TypeFor[*Recursive]()
