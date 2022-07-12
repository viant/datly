package view

import "reflect"

type Namer interface {
	Names(rField reflect.StructField) []string
}
