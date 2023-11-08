package session

import (
	"encoding/json"
	"github.com/viant/datly/utils/types"
	"github.com/viant/xunsafe"
	"reflect"
)

// Copier copies data between struct
type (
	Copier struct {
		dest *xunsafe.Struct
		src  map[string]*xunsafe.Field
	}

	copierOptions struct {
		debug bool
	}

	CopierOption func(o *copierOptions)
)

func WithDebug() CopierOption {
	return func(o *copierOptions) {
		o.debug = true
	}
}

// Copy copes field with the same name (initial version)
func (c *Copier) Copy(src interface{}, dest interface{}, opts ...CopierOption) error {
	options := &copierOptions{}
	for _, opt := range opts {
		opt(options)
	}

	srcPtr := xunsafe.AsPointer(src)
	destPtr := xunsafe.AsPointer(dest)
	for i := range c.dest.Fields {
		destField := &c.dest.Fields[i]
		srcField, ok := c.src[destField.Name]
		if !ok {
			continue
		}
		value := srcField.Value(srcPtr)
		isCompatible := srcField.Type.AssignableTo(destField.Type) || srcField.Type.ConvertibleTo(destField.Type)
		if options.debug && isCompatible {
			if aStruct := types.EnsureStruct(srcField.Type); aStruct != nil && aStruct.Name() == "" {
				isCompatible = false
			}
		}

		if isCompatible {
			destField.SetValue(destPtr, value)
			continue
		}
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}

		if destField.Kind() == reflect.Ptr {
			value = reflect.New(destField.Type.Elem()).Interface()
			if err = json.Unmarshal(data, value); err != nil {
				return err
			}
			destField.SetValue(destPtr, value)
			continue
		}

		value = destField.Addr(destPtr)
		if err = json.Unmarshal(data, value); err != nil {
			return err
		}
	}
	return nil
}

// NewCopier creates a copier
func NewCopier(src, dest reflect.Type) *Copier {
	src = types.EnsureStruct(src)
	dest = types.EnsureStruct(dest)
	ret := &Copier{src: make(map[string]*xunsafe.Field)}
	srcStruct := xunsafe.NewStruct(src)
	for i, item := range srcStruct.Fields {
		ret.src[item.Name] = &srcStruct.Fields[i]
	}
	ret.dest = xunsafe.NewStruct(dest)
	return ret
}
