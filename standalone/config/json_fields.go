package config

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"unicode"
)

type jsonField struct {
	name              string
	typ               reflect.Type
	index             []int
	tagged, ambiguous bool
}

type jsonFields []jsonField

func (f jsonFields) lookup(key string) *jsonField {
	for i := range f {
		if f[i].name == key {
			return &f[i]
		}
	}
	for i := range f {
		if strings.EqualFold(f[i].name, key) {
			return &f[i]
		}
	}
	return nil
}

func jsonObjectType(typ reflect.Type) reflect.Type {
	if typ == nil {
		return nil
	}
	// Custom JSON decoders own their property semantics, not the Go field layout.
	unmarshaler := reflect.TypeFor[json.Unmarshaler]()
	for typ.Kind() == reflect.Pointer {
		if typ.Implements(unmarshaler) {
			return nil
		}
		typ = typ.Elem()
	}
	if reflect.PointerTo(typ).Implements(unmarshaler) {
		return nil
	}
	return typ
}

// jsonStructFields mirrors encoding/json field selection: shallow fields win,
// explicit tags break same-depth ties, and ambiguous fields are ignored. Field
// index order determines the first case-insensitive match; exact matches win.
func jsonStructFields(typ reflect.Type) jsonFields {
	selected := map[string]jsonField{}
	ancestors := map[reflect.Type]bool{}
	var visit func(reflect.Type, []int)
	visit = func(typ reflect.Type, prefix []int) {
		if ancestors[typ] {
			return
		}
		ancestors[typ] = true
		defer delete(ancestors, typ)
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			base := field.Type
			if base.Kind() == reflect.Pointer {
				base = base.Elem()
			}
			if !field.IsExported() && !(field.Anonymous && base.Kind() == reflect.Struct) {
				continue
			}
			tag := field.Tag.Get("json")
			if tag == "-" {
				continue
			}
			name, _, _ := strings.Cut(tag, ",")
			if !validJSONFieldName(name) {
				name = ""
			}
			index := append(append([]int(nil), prefix...), i)
			if field.Anonymous && base.Kind() == reflect.Struct && name == "" {
				visit(base, index)
				continue
			}
			candidate := jsonField{name: name, typ: field.Type, index: index, tagged: name != ""}
			if name == "" {
				candidate.name = field.Name
			}
			previous, exists := selected[candidate.name]
			if !exists || len(index) < len(previous.index) || len(index) == len(previous.index) && candidate.tagged && !previous.tagged {
				selected[candidate.name] = candidate
			} else if len(index) == len(previous.index) && candidate.tagged == previous.tagged {
				previous.ambiguous = true
				selected[candidate.name] = previous
			}
		}
	}
	visit(typ, nil)
	var result jsonFields
	for _, field := range selected {
		if !field.ambiguous {
			result = append(result, field)
		}
	}
	slices.SortFunc(result, func(a, b jsonField) int { return slices.Compare(a.index, b.index) })
	return result
}

func validJSONFieldName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if !strings.ContainsRune("!#$%&()*+-./:;<=>?@[]^_{|}~ ", r) && !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}
