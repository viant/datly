package namedcontainers

import "reflect"

type Item struct{ ID int }
type Slice []Item
type Array [2]Item
type Map map[string]Item

var Anchors = []reflect.Type{reflect.TypeFor[*Slice](), reflect.TypeFor[*Array](), reflect.TypeFor[*Map]()}

func Local() reflect.Type { type local struct{ Hidden int }; return reflect.TypeFor[*local]() }
