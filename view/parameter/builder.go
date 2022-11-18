package parameter

import (
	"fmt"
	"reflect"
	"strings"
)

type (
	Builder struct {
		builders []*Builder
		field    []reflect.StructField
		index    map[string]int
		name     string
	}
)

func NewBuilder(name string) *Builder {
	return &Builder{
		index: map[string]int{},
		name:  name,
	}
}

func (b *Builder) AddType(path string, rType reflect.Type, options ...interface{}) error {
	var aTag reflect.StructTag
	for _, option := range options {
		switch actual := option.(type) {
		case reflect.StructTag:
			aTag = actual
		}
	}
	pathSegments := strings.Split(path, ".")
	return b.add(pathSegments, rType, aTag)
}

func (b *Builder) add(segments []string, rType reflect.Type, tag reflect.StructTag) error {
	if len(segments) == 1 {
		if segments[0] == "" {
			return fmt.Errorf("segment can't be empty")
		}

		pgkPath := b.pkgPath(segments[0])

		b.field = append(b.field, reflect.StructField{Name: segments[0], PkgPath: pgkPath, Type: rType, Tag: tag})
		return nil
	}

	return b.getOrCreate(segments[0]).add(segments[1:], rType, tag)
}

func (b *Builder) pkgPath(fieldName string) string {
	var pgkPath string
	if fieldName[0] > 'Z' || fieldName[0] < 'A' {
		pgkPath = "github.com/viant/datly/view/parameter"
	}

	return pgkPath
}

func (b *Builder) Build() reflect.Type {
	fields := make([]reflect.StructField, len(b.field)+len(b.builders))
	var i int
	for ; i < len(b.field); i++ {
		fields[i] = b.field[i]
	}

	var counter int
	for ; counter < len(b.builders); i++ {
		fields[i+counter] = reflect.StructField{
			Name:    b.builders[counter].name,
			PkgPath: b.pkgPath(b.builders[counter].name),
			Type:    b.builders[counter].Build(),
		}
		counter++
	}

	return reflect.StructOf(fields)
}

func (b *Builder) getOrCreate(name string) *Builder {
	index, ok := b.index[name]
	if !ok {
		b.index[name] = len(b.builders)
		index = len(b.builders)
		b.builders = append(b.builders, NewBuilder(name))
	}

	return b.builders[index]
}
