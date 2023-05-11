package tabjson

import (
	"bytes"
	io2 "github.com/viant/sqlx/io"
	"github.com/viant/xunsafe"
	"reflect"
)

type writer struct {
	beforeFirst   string
	writtenObject bool
	dereferencer  *xunsafe.Type
	buffer        *Buffer
	config        *Config
	accessor      *Accessor
	valueAt       io2.ValueAccessor
	size          int
}

func newWriter(accessor *Accessor, config *Config, buffer *Buffer, dereferencer *xunsafe.Type, valueAt io2.ValueAccessor, size int, beforeFirst string) *writer {
	return &writer{
		dereferencer: dereferencer,
		buffer:       buffer,
		config:       config,
		accessor:     accessor,
		valueAt:      valueAt,
		size:         size,
		beforeFirst:  beforeFirst,
	}
}

func (w *writer) writeAllObjects(acc *Accessor, parentLevel bool) {

	w.buffer.writeString("[")
	defer w.buffer.writeString("]")

	w.writeHeadersIfNeeded(acc.Headers())

	var xType *xunsafe.Type

	//fmt.Printf("\n*** SIZE = %d ***\n", w.size) // TODO check sizes

	for i := 0; i < w.size; i++ {
		acc.ResetAllChildren()

		if parentLevel {
			if i != 0 {
				acc.Reset()
			}

			at := w.valueAt(i)
			if i == 0 {
				if reflect.TypeOf(at).Kind() == reflect.Ptr {
					xType = w.dereferencer
				}
			}

			if xType != nil {
				at = xType.Deref(at)
			}

			acc.Set(xunsafe.AsPointer(at))
		}

		for acc.Has() {
			w.buffer.writeString(w.config.FieldSeparator)
			//w.buffer.writeString("#")
			//w.buffer.writeString("\n") // TODO MFI use for test formatting only
			w.buffer.writeString("[")

			result, wasStrings := acc.stringifyFields(w)
			w.writeObject(result, wasStrings)

			for _, child := range acc.children {
				w.buffer.writeString(w.config.FieldSeparator)
				//w.buffer.writeString("$")

				_, childSize := child.values()

				if childSize > 0 {
					tmpSize := w.size
					w.size = childSize
					w.writeAllObjects(child, false)
					w.size = tmpSize
				}

				if childSize == 0 {
					w.buffer.writeString("null")
				}
			}

			w.buffer.writeString("]")
		}
	}
}

func (w *writer) writeHeadersIfNeeded(headers []string) {
	if len(headers) == 0 {
		return
	}

	wasHeaderString := make([]bool, 0, len(headers))
	for range headers {
		wasHeaderString = append(wasHeaderString, true)
	}

	w.buffer.writeString("[")
	w.writeObject(headers, wasHeaderString)
	w.buffer.writeString("]")
}

func (w *writer) writeObject(data []string, wasStrings []bool) {
	if w.writtenObject {
		//w.writeObjectSeparator()
	} else {
		w.buffer.writeString(w.beforeFirst) // TODO w.beforeFirst [[??
	}

	WriteObject(w.buffer, w.config, data, wasStrings)
	w.writtenObject = true
}

func (w *writer) appendObject(data []string, wasStrings []bool) {
	if !bytes.HasSuffix(w.buffer.buffer, []byte(w.config.ObjectSeparator)) {
		w.buffer.writeString(w.config.FieldSeparator)
	}

	WriteObject(w.buffer, w.config, data, wasStrings)
}

func (w *writer) writeObjectSeparator() {
	w.buffer.writeString(w.config.ObjectSeparator) // TODO
}
