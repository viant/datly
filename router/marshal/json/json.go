package json

import (
	"bytes"
	"fmt"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/datly/shared"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unsafe"
)

const null = `null`
const defaultCaser = format.CaseUpperCamel

type (
	Marshaller struct {
		rType       reflect.Type
		marshallers []*fieldMarshaller
		config      marshal.Default

		cache sync.Map
	}

	fieldMarshaller struct {
		xField   *xunsafe.Field
		marshall marshallFieldFn
		fields   []*fieldMarshaller

		outputName string
		fieldName  string
		omitEmpty  bool

		path         string
		isComparable bool
		nilValue     interface{}
		zeroValue    interface{}
	}

	marshallObjFn   func(ptr unsafe.Pointer, fields []*fieldMarshaller, sb *bytes.Buffer, filters *Filters, path string) error
	marshallFieldFn func(unsafe.Pointer, *bytes.Buffer, *Filters) error
)

func New(rType reflect.Type, config marshal.Default) (*Marshaller, error) {
	return newMarshaller(rType, config, "")
}

func newMarshaller(rType reflect.Type, config marshal.Default, initialPath string) (*Marshaller, error) {
	json := &Marshaller{
		rType:  rType,
		config: config,
	}

	if err := json.init(initialPath); err != nil {
		return nil, err
	}

	return json, nil
}

func (j *Marshaller) init(initialPath string) error {
	marshallers, err := j.structMarshallers(j.rType, j.config, initialPath)
	if err != nil {
		return err
	}

	j.marshallers = marshallers
	return nil
}

func (j *Marshaller) structMarshallers(rType reflect.Type, config marshal.Default, path string) ([]*fieldMarshaller, error) {
	elem := shared.Elem(rType)

	numField := elem.NumField()

	marshallers := make([]*fieldMarshaller, 0)
	for i := 0; i < numField; i++ {
		err := j.newFieldMarshaller(&marshallers, elem.Field(i), config, path)
		if err != nil {
			return nil, err
		}

	}

	return marshallers, nil
}

func (j *Marshaller) newFieldMarshaller(marshallers *[]*fieldMarshaller, field reflect.StructField, config marshal.Default, path string) error {
	if field.Anonymous {
		anonymousMarshallers, err := j.structMarshallers(field.Type, config, path)
		if err != nil {
			return err
		}

		*marshallers = append(*marshallers, anonymousMarshallers...)
		return nil
	}

	tag := Parse(field.Tag.Get(TagName))
	if tag.FieldName == "-" {
		return nil
	}

	outputField := field.Name
	if outputField[0] > 'Z' || outputField[0] < 'A' && tag.FieldName == "" {
		return nil
	}

	if tag.FieldName != "" {
		outputField = tag.FieldName
	} else if config.CaseFormat != 0 {
		outputField = defaultCaser.Format(outputField, config.CaseFormat)
	}

	if path == "" {
		path = field.Name
	} else {
		path = path + "." + field.Name
	}

	xField := xunsafe.NewField(field)
	marshaller := &fieldMarshaller{
		outputName: outputField,
		fieldName:  field.Name,
		xField:     xField,
		omitEmpty:  tag.OmitEmpty || config.OmitEmpty,
		path:       path,
	}

	if err := marshaller.init(field, config, j); err != nil {
		return err
	}

	*marshallers = append(*marshallers, marshaller)

	return nil
}

func (f *fieldMarshaller) init(field reflect.StructField, config marshal.Default, j *Marshaller) error {
	defaultTag, err := NewDefaultTag(field)
	if err != nil {
		return err
	}

	var wasPtr bool
	rType := field.Type
	if field.Type.Kind() == reflect.Ptr {
		wasPtr = true
		rType = rType.Elem()
	}

	f.isComparable = rType.Kind() != reflect.Slice
	if wasPtr || rType.Kind() == reflect.Slice {
		f.nilValue = reflect.New(field.Type).Elem().Interface()
	}

	f.zeroValue = reflect.New(field.Type).Elem().Interface()

	switch rType.Kind() {
	case reflect.Int:
		updateIntMarshaller(f, wasPtr, defaultTag)
	case reflect.Int8:
		updateInt8Marshaller(f, wasPtr, defaultTag)
	case reflect.Int16:
		updateInt16Marshaller(f, wasPtr, defaultTag)
	case reflect.Int32:
		updateInt32Marshaller(f, wasPtr, defaultTag)
	case reflect.Int64:
		updateInt64Marshaller(f, wasPtr, defaultTag)
	case reflect.Uint:
		updateUintMarshaller(f, wasPtr, defaultTag)
	case reflect.Uint8:
		updateUint8Marshaller(f, wasPtr, defaultTag)
	case reflect.Uint16:
		updateUint16Marshaller(f, wasPtr, defaultTag)
	case reflect.Uint32:
		updateUint32Marshaller(f, wasPtr, defaultTag)
	case reflect.Uint64:
		updateUint64Marshaller(f, wasPtr, defaultTag)
	case reflect.Bool:
		updateBoolMarshaller(f, wasPtr, defaultTag)
	case reflect.String:
		updateStringMarshaller(f, wasPtr, defaultTag)
	case reflect.Float64:
		updateFloat64Marshaller(f, wasPtr, defaultTag)
	case reflect.Float32:
		updateFloat32Marshaller(f, wasPtr, defaultTag)
	case reflect.Slice, reflect.Struct:

		if rType.Kind() == reflect.Slice && rType.Elem().Kind() == reflect.Uint8 {
			updateBytesMarshaller(f, wasPtr, defaultTag)
		} else if rType == timeType {
			updateTimeMarshaller(f, wasPtr, defaultTag)
		} else {
			if err := j.updateNonPrimitiveMarshaller(f); err != nil {
				return err
			}
			childrenPath := f.fullPath()

			marshallers, err := j.structMarshallers(rType, config, childrenPath)
			if err != nil {
				return err
			}
			f.fields = marshallers
		}

	case reflect.Interface:
		f.updateInterfaceMarshaller(config, j)

	default:
		return fmt.Errorf("unsupported type %v", field.Type.String())
	}
	return nil
}

func (f *fieldMarshaller) updateInterfaceMarshaller(config marshal.Default, j *Marshaller) {
	f.marshall = func(ptr unsafe.Pointer, buffer *bytes.Buffer, filters *Filters) error {
		asInterface := f.xField.Value(ptr)
		interfaceType := reflect.TypeOf(asInterface)
		for interfaceType.Kind() == reflect.Ptr {
			interfaceType = interfaceType.Elem()
		}

		for interfaceType.Kind() == reflect.Slice {
			interfaceType = interfaceType.Elem()
		}

		marshaller, ok := j.cache.Load(interfaceType)
		if ok {
			return marshaller.(*Marshaller).stringifyValue(xunsafe.AsPointer(asInterface), filters, interfaceType, buffer, f.fullPath())
		}

		interfaceMarshaller, err := newMarshaller(interfaceType, config, f.fullPath())
		if err != nil {
			return err
		}

		j.cache.Store(interfaceType, interfaceMarshaller)
		return interfaceMarshaller.stringifyValue(xunsafe.AsPointer(asInterface), filters, interfaceType, buffer, f.fullPath())
	}
}

func (f *fieldMarshaller) fullPath() string {
	return f.path
}

func updateTimeMarshaller(f *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	timeFormat := time.RFC3339
	if tag.Format != "" {
		timeFormat = tag.Format
	}

	if wasPtr {
		f.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			timePtr := f.xField.TimePtr(ptr)
			return appendTime(sb, timePtr, tag, timeFormat)
		}
		return
	}

	f.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		aTime := f.xField.Time(ptr)
		return appendTime(sb, &aTime, tag, timeFormat)
	}
}

func appendTime(sb *bytes.Buffer, aTime *time.Time, tag *DefaultTag, timeFormat string) error {
	if (aTime == nil || aTime.IsZero()) && tag._value != nil {
		aTime = tag._value.(*time.Time)
	}

	if aTime != nil {
		sb.WriteByte('"')
		sb.WriteString(aTime.Format(timeFormat))
		sb.WriteByte('"')
		return nil
	}

	sb.WriteString(null)
	return nil
}

func (j *Marshaller) updateNonPrimitiveMarshaller(stringifier *fieldMarshaller) error {
	marshaller, err := j.stringifyNonPrimitive(stringifier.xField.Type)

	if err != nil {
		return err
	}

	stringifier.marshall = func(pointer unsafe.Pointer, sb *bytes.Buffer, filters *Filters) error {
		return marshaller(xunsafe.AsPointer(stringifier.xField.Value(pointer)), stringifier.fields, sb, filters, stringifier.path)
	}
	return nil
}

func updateFloat32Marshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			float32Ptr := stringifier.xField.Float32Ptr(ptr)
			if float32Ptr == nil {
				return appendFloat(sb, 0, true, tag)
			}

			return appendFloat(sb, float64(*float32Ptr), false, tag)
		}

		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		return appendFloat(sb, float64(stringifier.xField.Float32(ptr)), false, tag)
	}
}

func updateFloat64Marshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			floatPtr := stringifier.xField.Float64Ptr(ptr)
			if floatPtr == nil {
				return appendFloat(sb, 0, true, tag)
			}

			return appendFloat(sb, *floatPtr, false, tag)
		}

		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		return appendFloat(sb, stringifier.xField.Float64(ptr), false, tag)
	}
}

func appendFloat(sb *bytes.Buffer, f float64, wasNull bool, tag *DefaultTag) error {
	if f == 0 && tag._value != nil {
		sb.WriteString(strconv.FormatFloat(tag._value.(float64), 'f', -1, 64))
		return nil
	}

	if wasNull {
		sb.WriteString("0")
		return nil
	}

	sb.WriteString(strconv.FormatFloat(f, 'f', -1, 64))
	return nil
}

func updateStringMarshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			stringPtr := stringifier.xField.StringPtr(ptr)
			if stringPtr == nil {
				if tag._value != nil {
					sb.WriteString(tag._value.(string))
					return nil
				}

				sb.WriteString(`""`)
				return nil
			}

			marshallString(sb, *stringPtr)
			return nil
		}

		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		aString := stringifier.xField.String(ptr)
		if aString == "" && tag._value != nil {
			aString = tag._value.(string)
		}

		marshallString(sb, aString)
		return nil
	}
}

func updateBytesMarshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			bytesPtr := stringifier.xField.BytesPtr(ptr)
			if bytesPtr == nil {
				if tag._value != nil {
					sb.Write(tag._value.([]byte))
					return nil
				}

				sb.WriteString(null)
				return nil
			}

			marshallString(sb, string(*bytesPtr))
			return nil
		}

		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		aString := stringifier.xField.String(ptr)
		if aString == "" && tag._value != nil {
			aString = tag._value.(string)
		}

		marshallString(sb, aString)
		return nil
	}
}

func marshallString(sb *bytes.Buffer, asString string) {
	asString = strings.TrimFunc(asString, func(r rune) bool {
		return !unicode.IsGraphic(r)
	})
	sb.WriteByte('"')
	if strings.Contains(asString, `"`) {
		sb.WriteString(strings.ReplaceAll(strings.ReplaceAll(asString, `\`, `\\`), `"`, `\"`))
	} else {
		sb.WriteString(asString)
	}

	sb.WriteByte('"')
}

func updateBoolMarshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			bPtr := stringifier.xField.BoolPtr(pointer)
			if bPtr == nil {
				if tag._value != nil {
					marshallBool(tag._value.(bool), sb)
					return nil
				}

				sb.WriteString("false")
				return nil
			}

			marshallBool(*bPtr, sb)
			return nil
		}

		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		marshallBool(stringifier.xField.Bool(ptr), sb)
		return nil
	}
}

func marshallBool(b bool, sb *bytes.Buffer) {
	if b == true {
		sb.WriteString(`true`)
	} else {
		sb.WriteString(`false`)
	}
}

func updateUint64Marshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.Uint64Ptr(pointer)
			if intPtr == nil {
				return appendInt(0, true, tag, sb)
			}

			return appendInt(int(*intPtr), false, tag, sb)
		}
		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		return appendInt(int(stringifier.xField.Uint64(ptr)), false, tag, sb)
	}
}

func updateUint32Marshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.Uint32Ptr(pointer)
			if intPtr == nil {
				return appendInt(0, true, tag, sb)
			}

			return appendInt(int(*intPtr), false, tag, sb)
		}
		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		return appendInt(int(stringifier.xField.Uint32(ptr)), false, tag, sb)
	}
}

func updateUint16Marshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.Uint16Ptr(pointer)
			if intPtr == nil {
				return appendInt(0, true, tag, sb)
			}

			return appendInt(int(*intPtr), false, tag, sb)
		}
		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		return appendInt(int(stringifier.xField.Uint16(ptr)), false, tag, sb)
	}
}

func updateUint8Marshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.Uint8Ptr(pointer)
			if intPtr == nil {
				return appendInt(0, true, tag, sb)
			}

			return appendInt(int(*intPtr), false, tag, sb)
		}
		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		return appendInt(int(stringifier.xField.Uint8(ptr)), false, tag, sb)
	}
}

func updateUintMarshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.UintPtr(pointer)
			if intPtr == nil {
				return appendInt(0, true, tag, sb)
			}

			return appendInt(int(*intPtr), false, tag, sb)
		}
		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		return appendInt(int(stringifier.xField.Uint(ptr)), false, tag, sb)
	}
}

func updateInt64Marshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.Int64Ptr(pointer)
			if intPtr == nil {
				return appendInt(0, true, tag, sb)
			}

			return appendInt(int(*intPtr), false, tag, sb)
		}
		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		return appendInt(int(stringifier.xField.Int64(ptr)), false, tag, sb)
	}
}

func updateInt32Marshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.Int32Ptr(pointer)
			if intPtr == nil {
				return appendInt(0, true, tag, sb)
			}

			return appendInt(int(*intPtr), false, tag, sb)
		}
		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		return appendInt(int(stringifier.xField.Int32(ptr)), false, tag, sb)
	}
}

func updateInt16Marshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.Int16Ptr(pointer)
			if intPtr == nil {
				return appendInt(0, true, tag, sb)
			}

			return appendInt(int(*intPtr), false, tag, sb)
		}
		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		return appendInt(int(stringifier.xField.Int16(ptr)), false, tag, sb)
	}
}

func updateInt8Marshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.Int8Ptr(pointer)
			if intPtr == nil {
				return appendInt(0, true, tag, sb)
			}

			return appendInt(int(*intPtr), false, tag, sb)
		}
		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		return appendInt(int(stringifier.xField.Int8(ptr)), false, tag, sb)
	}
}

func updateIntMarshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.IntPtr(pointer)
			if intPtr == nil {
				return appendInt(0, true, tag, sb)
			}

			return appendInt(*intPtr, false, tag, sb)
		}
		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		return appendInt(stringifier.xField.Int(ptr), false, tag, sb)
	}
}

func appendInt(value int, wasNull bool, aTag *DefaultTag, sb *bytes.Buffer) error {
	if aTag._value != nil && value == 0 {
		sb.WriteString(strconv.Itoa(aTag._value.(int)))
		return nil
	}

	if wasNull {
		sb.WriteString("0")
		return nil
	}

	sb.WriteString(strconv.Itoa(value))
	return nil
}

func (j *Marshaller) asObject(ptr unsafe.Pointer, fields []*fieldMarshaller, sb *bytes.Buffer, filters *Filters, path string) error {
	if ptr == nil {
		sb.WriteString(null)
		return nil
	}

	filter, _ := filterByPath(filters, path)

	counter := 0
	sb.WriteByte('{')
	for _, stringifier := range fields {
		if isExcluded(filter, stringifier.fieldName) {
			continue
		}

		value := stringifier.xField.Value(ptr)
		isZeroVal := isZeroValue(ptr, stringifier, value)
		if stringifier.omitEmpty && isZeroVal {
			continue
		}

		if counter > 0 {
			sb.WriteByte(',')
		}

		sb.WriteByte('"')
		sb.WriteString(stringifier.outputName)
		sb.WriteString(`":`)
		if err := stringifier.marshall(ptr, sb, filters); err != nil {
			return err
		}

		counter++
	}

	sb.WriteByte('}')
	return nil
}

func isZeroValue(ptr unsafe.Pointer, stringifier *fieldMarshaller, value interface{}) bool {
	if stringifier.isComparable {
		return stringifier.zeroValue == value
	}

	t := stringifier.xField.Type
	valuePtr := stringifier.xField.Pointer(ptr)
	switch t.Kind() {
	case reflect.Ptr:
		return valuePtr == nil
	case reflect.Slice:
		s := (*reflect.SliceHeader)(valuePtr)
		return s != nil && s.Len == 0
	}

	//this should not happen, all the cases should be covered earlier
	return false
}

func isExcluded(filter Filter, name string) bool {
	if filter == nil {
		return false
	}

	_, ok := filter[name]
	return !ok
}

func (j *Marshaller) Marshal(value interface{}, filters *Filters) ([]byte, error) {
	if value == nil {
		return []byte(null), nil
	}

	rType := reflect.TypeOf(value)
	if j.rType != rType && rType != reflect.SliceOf(j.rType) {
		return nil, fmt.Errorf("type missmatch, wanted %v or %v but got %v", j.rType.String(), reflect.SliceOf(j.rType).String(), rType.String())
	}

	if rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}

	buffer := bufferPool.Get()
	if err := j.stringifyValue(xunsafe.AsPointer(value), filters, rType, buffer, ""); err != nil {
		return nil, err
	}

	output := make([]byte, len(buffer.Bytes()))
	copy(output, buffer.Bytes())

	bufferPool.Put(buffer)
	return output, nil
}

func (j *Marshaller) stringifyValue(ptr unsafe.Pointer, filters *Filters, rType reflect.Type, buffer *bytes.Buffer, path string) error {
	stringifier, err := j.stringifyNonPrimitive(rType)
	if err != nil {
		return err
	}

	if err = stringifier(ptr, j.marshallers, buffer, filters, path); err != nil {
		return err
	}
	return nil
}

func (j *Marshaller) stringifyNonPrimitive(rType reflect.Type) (marshallObjFn, error) {
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}

	switch rType.Kind() {
	case reflect.Struct:
		return j.asObject, nil
	case reflect.Slice:
		return j.storeOrLoadSliceMarshaller(rType)
	default:
		return nil, fmt.Errorf("unsupported type %v", rType)
	}
}

func (j *Marshaller) storeOrLoadSliceMarshaller(rType reflect.Type) (marshallObjFn, error) {
	encoder, ok := sliceStringifier.Load(rType)
	if ok {
		if marshaller, ok := encoder.(marshallObjFn); ok {
			return marshaller, nil
		}
	}

	marshaller := j.asSlice(rType)
	sliceStringifier.Store(rType, marshaller)
	return marshaller, nil
}

func (j *Marshaller) asSlice(rType reflect.Type) marshallObjFn {
	xslice := xunsafe.NewSlice(rType)
	return func(ptr unsafe.Pointer, fields []*fieldMarshaller, sb *bytes.Buffer, filters *Filters, path string) error {
		s := (*reflect.SliceHeader)(ptr)
		if s != nil && s.Data == 0 {
			sb.WriteString("[]")
			return nil
		}

		sb.WriteByte('[')
		for i := 0; i < xslice.Len(ptr); i++ {
			if i != 0 {
				sb.WriteByte(',')
			}
			if err := j.asObject(xunsafe.AsPointer(xslice.ValuePointerAt(ptr, i)), fields, sb, filters, path); err != nil {
				return err
			}
		}
		sb.WriteByte(']')

		return nil
	}
}

func filterByPath(filters *Filters, path string) (Filter, bool) {
	if filters == nil {
		return nil, false
	}

	filter, ok := filters.fields[path]
	return filter, ok
}
