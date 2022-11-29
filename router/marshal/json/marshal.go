package json

import (
	"bytes"
	goJson "encoding/json"
	"fmt"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/datly/shared"
	"github.com/viant/toolbox/format"
	"github.com/viant/xreflect"
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
const IndexKey = "jsonIndex"

type (
	Marshaller struct {
		rType       reflect.Type
		marshallers []*fieldMarshaller
		config      marshal.Default

		cache              sync.Map
		sliceStringifier   sync.Map
		_marshallersOutput map[string]int
		_marshallersInput  map[string]int
	}

	fieldMarshaller struct {
		xField   *xunsafe.Field
		xType    *xunsafe.Type
		marshall marshallFieldFn
		fields   []*fieldMarshaller

		outputName string
		fieldName  string
		omitEmpty  bool

		path             string
		isComparable     bool
		zeroValue        interface{}
		outputPath       string
		derefCount       int
		indirectAccessor *xunsafe.Field
		derefStart       int
		indexUpdater     *presenceUpdater
	}

	marshallObjFn   func(parentType reflect.Type, ptr unsafe.Pointer, fields []*fieldMarshaller, sb *bytes.Buffer, filters *Filters, path string) error
	marshallFieldFn func(reflect.Type, unsafe.Pointer, *bytes.Buffer, *Filters) error
)

func New(rType reflect.Type, config marshal.Default) (*Marshaller, error) {
	return newMarshaller(rType, config, "")
}

func newMarshaller(rType reflect.Type, config marshal.Default, initialPath string) (*Marshaller, error) {
	json := &Marshaller{
		rType:            rType,
		config:           config,
		sliceStringifier: sync.Map{},
		cache:            sync.Map{},
	}

	if err := json.init(initialPath); err != nil {
		return nil, err
	}

	return json, nil
}

func (j *Marshaller) init(initialPath string) error {
	marshallers, err := j.structMarshallers(j.rType, j.config, initialPath, initialPath, &DefaultTag{})
	if err != nil {
		return err
	}

	j.marshallers = marshallers
	j.indexMarshallers()
	return nil
}

func (j *Marshaller) structMarshallers(rType reflect.Type, config marshal.Default, path, outputPath string, dTag *DefaultTag) ([]*fieldMarshaller, error) {
	var wasPtr bool
	if rType.Kind() == reflect.Ptr {
		wasPtr = true
	}

	elem := shared.Elem(rType)
	if elem.Kind() != reflect.Struct {
		aField := &fieldMarshaller{
			xField: xunsafe.NewField(reflect.StructField{Name: "TEMP", Type: elem}),
		}

		if err := aField.updateFieldMarshaller(rType, config, j, elem, wasPtr, dTag); err != nil {
			return nil, err
		}

		return []*fieldMarshaller{aField}, nil
	}

	marshallers := make([]*fieldMarshaller, 0)
	var iUpdater *presenceUpdater

	numField := elem.NumField()
	for i := 0; i < numField; i++ {
		field := elem.Field(i)
		indexTag := field.Tag.Get(IndexKey)
		if indexTag != "" {
			var err error
			iUpdater, err = j.presenceUpdater(field)
			if err != nil {
				return nil, err
			}

			continue
		}

		dTag, err := NewDefaultTag(field)
		if err != nil {
			return nil, err
		}

		if err = j.newFieldMarshaller(&marshallers, field, config, path, outputPath, dTag); err != nil {
			return nil, err
		}
	}

	if iUpdater != nil {
		for _, marshaller := range marshallers {
			marshaller.indexUpdater = iUpdater
		}
	}

	return marshallers, nil
}

func (j *Marshaller) presenceUpdater(field reflect.StructField) (*presenceUpdater, error) {
	presenceFields, err := getFields(field.Type)
	if err != nil {
		return nil, err
	}

	presenceFieldsIndex := map[string]*xunsafe.Field{}
	for i, presenceField := range presenceFields {
		presenceFieldsIndex[presenceField.Name] = presenceFields[i]
	}

	iUpdater := &presenceUpdater{
		xField: xunsafe.NewField(field),
		fields: presenceFieldsIndex,
	}
	return iUpdater, nil
}

func getFields(rType reflect.Type) ([]*xunsafe.Field, error) {
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}

	if rType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("index has to be type of struct")
	}

	numField := rType.NumField()
	result := make([]*xunsafe.Field, 0, numField)
	for i := 0; i < numField; i++ {
		aField := rType.Field(i)
		if aField.Type != xreflect.BoolType {
			continue
		}

		result = append(result, xunsafe.NewField(aField))
	}
	return result, nil
}

func (j *Marshaller) newFieldMarshaller(marshallers *[]*fieldMarshaller, field reflect.StructField, config marshal.Default, path, outputPath string, defaultTag *DefaultTag) error {
	if field.Anonymous {
		rType, ptrSize := field.Type, 0
		for rType.Kind() == reflect.Ptr {
			rType = rType.Elem()
			ptrSize++
		}

		actualParentType := elem(rType)
		ptrStart := 0
		if actualParentType.NumField() == 1 {
			ptrStart = ptrSize
		}

		anonymousMarshallers, err := j.structMarshallers(rType, config, path, outputPath, defaultTag)
		if err != nil {
			return err
		}

		for _, marshaller := range anonymousMarshallers {
			if ptrSize == 0 {
				marshaller.xField.Offset += field.Offset
			} else {
				marshaller.indirectAccessor = xunsafe.NewField(field)
				marshaller.derefCount = ptrSize
				marshaller.derefStart = ptrStart
			}
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

	path, outputPath = addToPath(path, field.Name), addToPath(outputPath, outputField)

	xField := xunsafe.NewField(field)
	marshaller := &fieldMarshaller{
		outputName: outputField,
		fieldName:  field.Name,
		xField:     xField,
		omitEmpty:  tag.OmitEmpty || config.OmitEmpty,
		path:       path,
		outputPath: outputPath,
		xType:      xunsafe.NewType(reflect.PtrTo(xField.Type)),
	}

	if err := marshaller.init(field, config, j); err != nil {
		return err
	}

	*marshallers = append(*marshallers, marshaller)

	return nil
}

func addToPath(path, field string) string {
	if path == "" {
		return field
	}
	return path + "." + field
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

	f.isComparable = rType.Comparable()
	f.zeroValue = reflect.New(field.Type).Elem().Interface()

	err = f.updateFieldMarshaller(field.Type, config, j, rType, wasPtr, defaultTag)
	if err != nil {
		return err
	}
	return nil
}

func (f *fieldMarshaller) updateFieldMarshaller(parentType reflect.Type, config marshal.Default, j *Marshaller, rType reflect.Type, wasPtr bool, defaultTag *DefaultTag) error {
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
			updateTimeMarshaller(f, wasPtr, defaultTag, config)
		} else {
			marshallers, err := j.structMarshallers(rType, config, f.path, f.outputPath, defaultTag)
			if err != nil {
				return err
			}

			if err := j.updateNonPrimitiveMarshaller(f); err != nil {
				return err
			}
			f.fields = marshallers
		}

	case reflect.Interface:
		f.updateInterfaceMarshaller(config, j)

	default:
		return fmt.Errorf("unsupported type %v", parentType.String())
	}

	return nil
}

func (f *fieldMarshaller) updateInterfaceMarshaller(config marshal.Default, j *Marshaller) {
	f.marshall = func(interfaceType reflect.Type, ptr unsafe.Pointer, buffer *bytes.Buffer, filters *Filters) error {
		asInterface := f.xField.Value(ptr)
		for interfaceType.Kind() == reflect.Ptr {
			interfaceType = interfaceType.Elem()
		}

		for interfaceType.Kind() == reflect.Slice {
			interfaceType = interfaceType.Elem()
		}

		marshaller, ok := j.cache.Load(interfaceType)
		if ok {
			return marshaller.(*Marshaller).stringifyValue(interfaceType, xunsafe.AsPointer(asInterface), filters, interfaceType, buffer, f.path)
		}

		interfaceMarshaller, err := newMarshaller(interfaceType, config, f.path)
		if err != nil {
			return err
		}

		j.cache.Store(interfaceType, interfaceMarshaller)
		return interfaceMarshaller.stringifyValue(interfaceType, xunsafe.AsPointer(asInterface), filters, interfaceType, buffer, f.path)
	}
}

func updateTimeMarshaller(f *fieldMarshaller, wasPtr bool, tag *DefaultTag, config marshal.Default) {
	timeFormat := time.RFC3339
	if tag.Format != "" {
		timeFormat = tag.Format
	}

	if config.DateLayout != "" {
		timeFormat = config.DateLayout
	}

	if wasPtr {
		f.marshall = func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			timePtr := f.xField.TimePtr(ptr)
			return appendTime(sb, timePtr, tag, timeFormat)
		}
		return
	}

	f.marshall = func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
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

	stringifier.marshall = func(parentType reflect.Type, pointer unsafe.Pointer, sb *bytes.Buffer, filters *Filters) error {
		return marshaller(parentType, xunsafe.AsPointer(stringifier.xField.Value(pointer)), stringifier.fields, sb, filters, stringifier.path)
	}
	return nil
}

func updateFloat32Marshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			float32Ptr := stringifier.xField.Float32Ptr(ptr)
			if float32Ptr == nil {
				return appendFloat(sb, 0, true, tag)
			}

			return appendFloat(sb, float64(*float32Ptr), false, tag)
		}

		return
	}

	stringifier.marshall = func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		return appendFloat(sb, float64(stringifier.xField.Float32(ptr)), false, tag)
	}
}

func updateFloat64Marshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			floatPtr := stringifier.xField.Float64Ptr(ptr)
			if floatPtr == nil {
				return appendFloat(sb, 0, true, tag)
			}

			return appendFloat(sb, *floatPtr, false, tag)
		}

		return
	}

	stringifier.marshall = func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		return appendFloat(sb, stringifier.xField.Float64(ptr), false, tag)
	}
}

func appendFloat(sb *bytes.Buffer, f float64, wasNull bool, tag *DefaultTag) error {
	if wasNull {
		sb.WriteString(null)
		return nil
	}

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
		defaultValue := null
		if tag._value != nil {
			defaultValue = tag._value.(string)
		}

		stringifier.marshall = func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			stringPtr := stringifier.xField.StringPtr(ptr)
			if stringPtr == nil {
				sb.WriteString(defaultValue)
				return nil
			}

			marshallString(sb, *stringPtr)
			return nil
		}

		return
	}

	stringifier.marshall = func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
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
		stringifier.marshall = func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
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

	stringifier.marshall = func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
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
		stringifier.marshall = func(parentType reflect.Type, pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			bPtr := stringifier.xField.BoolPtr(pointer)
			if bPtr == nil {
				if tag._value != nil {
					marshallBool(tag._value.(bool), sb)
					return nil
				}

				sb.WriteString(null)
				return nil
			}

			marshallBool(*bPtr, sb)
			return nil
		}

		return
	}

	stringifier.marshall = func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
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
		stringifier.marshall = func(parentType reflect.Type, pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.Uint64Ptr(pointer)
			if intPtr == nil {
				return appendInt(0, true, tag, sb)
			}

			return appendInt(int(*intPtr), false, tag, sb)
		}
		return
	}

	stringifier.marshall = func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		return appendInt(int(stringifier.xField.Uint64(ptr)), false, tag, sb)
	}
}

func updateUint32Marshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(parentType reflect.Type, pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.Uint32Ptr(pointer)
			if intPtr == nil {
				return appendInt(0, true, tag, sb)
			}

			return appendInt(int(*intPtr), false, tag, sb)
		}
		return
	}

	stringifier.marshall = func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		return appendInt(int(stringifier.xField.Uint32(ptr)), false, tag, sb)
	}
}

func updateUint16Marshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(parentType reflect.Type, pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.Uint16Ptr(pointer)
			if intPtr == nil {
				return appendInt(0, true, tag, sb)
			}

			return appendInt(int(*intPtr), false, tag, sb)
		}
		return
	}

	stringifier.marshall = func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		return appendInt(int(stringifier.xField.Uint16(ptr)), false, tag, sb)
	}
}

func updateUint8Marshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(parentType reflect.Type, pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.Uint8Ptr(pointer)
			if intPtr == nil {
				return appendInt(0, true, tag, sb)
			}

			return appendInt(int(*intPtr), false, tag, sb)
		}
		return
	}

	stringifier.marshall = func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		return appendInt(int(stringifier.xField.Uint8(ptr)), false, tag, sb)
	}
}

func updateUintMarshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(parentType reflect.Type, pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.UintPtr(pointer)
			if intPtr == nil {
				return appendInt(0, true, tag, sb)
			}

			return appendInt(int(*intPtr), false, tag, sb)
		}
		return
	}

	stringifier.marshall = func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		return appendInt(int(stringifier.xField.Uint(ptr)), false, tag, sb)
	}
}

func updateInt64Marshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(parentType reflect.Type, pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.Int64Ptr(pointer)
			if intPtr == nil {
				return appendInt(0, true, tag, sb)
			}

			return appendInt(int(*intPtr), false, tag, sb)
		}
		return
	}

	stringifier.marshall = func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		return appendInt(int(stringifier.xField.Int64(ptr)), false, tag, sb)
	}
}

func updateInt32Marshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(parentType reflect.Type, pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.Int32Ptr(pointer)
			if intPtr == nil {
				return appendInt(0, true, tag, sb)
			}

			return appendInt(int(*intPtr), false, tag, sb)
		}
		return
	}

	stringifier.marshall = func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		return appendInt(int(stringifier.xField.Int32(ptr)), false, tag, sb)
	}
}

func updateInt16Marshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(parentType reflect.Type, pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.Int16Ptr(pointer)
			if intPtr == nil {
				return appendInt(0, true, tag, sb)
			}

			return appendInt(int(*intPtr), false, tag, sb)
		}
		return
	}

	stringifier.marshall = func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		return appendInt(int(stringifier.xField.Int16(ptr)), false, tag, sb)
	}
}

func updateInt8Marshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(parentType reflect.Type, pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.Int8Ptr(pointer)
			if intPtr == nil {
				return appendInt(0, true, tag, sb)
			}

			return appendInt(int(*intPtr), false, tag, sb)
		}
		return
	}

	stringifier.marshall = func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		return appendInt(int(stringifier.xField.Int8(ptr)), false, tag, sb)
	}
}

func updateIntMarshaller(stringifier *fieldMarshaller, wasPtr bool, tag *DefaultTag) {
	if wasPtr {
		stringifier.marshall = func(parentType reflect.Type, pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.IntPtr(pointer)
			if intPtr == nil {
				return appendInt(0, true, tag, sb)
			}

			return appendInt(*intPtr, false, tag, sb)
		}
		return
	}

	stringifier.marshall = func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		return appendInt(stringifier.xField.Int(ptr), false, tag, sb)
	}
}

func appendInt(value int, wasNull bool, aTag *DefaultTag, sb *bytes.Buffer) error {
	if wasNull {
		sb.WriteString(null)
		return nil
	}

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

func (j *Marshaller) asObject(_ reflect.Type, ptr unsafe.Pointer, fields []*fieldMarshaller, sb *bytes.Buffer, filters *Filters, path string) error {
	if ptr == nil {
		sb.WriteString(null)
		return nil
	}

	filter, _ := filterByPath(filters, path)

	counter := 0
	sb.WriteByte('{')
	for _, stringifier := range fields {
		if isExcluded(filter, stringifier.fieldName, j.config, stringifier.path) {
			continue
		}

		objPtr := ptr
		if stringifier.indirectAccessor != nil {
			objPtr = stringifier.indirectAccessor.Pointer(objPtr)
			for i := stringifier.derefStart; i < stringifier.derefCount; i++ {
				objPtr = xunsafe.DerefPointer(objPtr)
			}
		}

		value := stringifier.xField.Value(objPtr)
		isZeroVal := isZeroValue(objPtr, stringifier, value)
		if stringifier.omitEmpty && isZeroVal {
			continue
		}

		if counter > 0 {
			sb.WriteByte(',')
		}
		counter++

		sb.WriteByte('"')
		sb.WriteString(stringifier.outputName)
		sb.WriteString(`":`)

		rType := stringifier.xField.Type
		if rType.Kind() == reflect.Interface {
			rType = reflect.TypeOf(value)
		}

		if err := stringifier.marshall(rType, objPtr, sb, filters); err != nil {
			return err
		}
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

func isExcluded(filter Filter, name string, config marshal.Default, path string) bool {
	if config.Exclude != nil {
		if _, ok := config.Exclude[path]; ok {
			return true
		}
	}

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
	if err := j.stringifyValue(rType, xunsafe.AsPointer(value), filters, rType, buffer, ""); err != nil {
		return nil, err
	}

	output := make([]byte, len(buffer.Bytes()))
	copy(output, buffer.Bytes())

	bufferPool.Put(buffer)
	return output, nil
}

func (j *Marshaller) stringifyValue(parentType reflect.Type, ptr unsafe.Pointer, filters *Filters, rType reflect.Type, buffer *bytes.Buffer, path string) error {
	stringifier, err := j.stringifyNonPrimitive(rType)
	if err != nil {
		return err
	}

	if err = stringifier(parentType, ptr, j.marshallers, buffer, filters, path); err != nil {
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
		return func(parentType reflect.Type, ptr unsafe.Pointer, fields []*fieldMarshaller, sb *bytes.Buffer, filters *Filters, path string) error {
			var asIface interface{}
			if parentType.Kind() == reflect.Interface {
				asIface = xunsafe.AsInterface(ptr)
			} else {
				asIface = GetXType(parentType).Interface(ptr)
			}
			ifaceMarshal, err := goJson.Marshal(asIface)
			if err != nil {
				return err
			}
			sb.Write(ifaceMarshal)
			return nil
		}, nil
	}
}

func (j *Marshaller) storeOrLoadSliceMarshaller(rType reflect.Type) (marshallObjFn, error) {
	encoder, ok := j.sliceStringifier.Load(rType)
	if ok {
		if marshaller, ok := encoder.(marshallObjFn); ok {
			return marshaller, nil
		}
	}

	marshaller := j.asSlice(rType)
	j.sliceStringifier.Store(rType, marshaller)
	return marshaller, nil
}

func (j *Marshaller) asSlice(rType reflect.Type) marshallObjFn {
	xslice := xunsafe.NewSlice(rType)
	rType = deref(rType.Elem())

	if rType.Kind() == reflect.Struct {
		return func(parentType reflect.Type, ptr unsafe.Pointer, fields []*fieldMarshaller, sb *bytes.Buffer, filters *Filters, path string) error {
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
				if err := j.asObject(parentType, xunsafe.AsPointer(xslice.ValuePointerAt(ptr, i)), fields, sb, filters, path); err != nil {
					return err
				}
			}
			sb.WriteByte(']')

			return nil
		}
	}

	return func(parentType reflect.Type, ptr unsafe.Pointer, fields []*fieldMarshaller, sb *bytes.Buffer, filters *Filters, path string) error {
		s := (*reflect.SliceHeader)(ptr)
		if s != nil && s.Data == 0 {
			sb.WriteString("[]")
			return nil
		}

		var err error
		sb.WriteByte('[')
		for i := 0; i < xslice.Len(ptr); i++ {
			if i != 0 {
				sb.WriteByte(',')
			}

			at := xslice.ValueAt(ptr, i)
			ifaceType := reflect.TypeOf(at)

			if err = fields[0].marshall(ifaceType, xunsafe.AsPointer(xslice.ValuePointerAt(ptr, i)), sb, filters); err != nil {
				return err
			}
		}

		sb.WriteByte(']')
		return nil
	}
}

func deref(rType reflect.Type) reflect.Type {
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}

	return rType
}

func (j *Marshaller) indexMarshallers() {
	j._marshallersOutput = map[string]int{}
	j._marshallersInput = map[string]int{}
	for i := range j.marshallers {
		j._marshallersOutput[j.marshallers[i].outputPath] = i
		j._marshallersInput[j.marshallers[i].path] = i
	}
}

func filterByPath(filters *Filters, path string) (Filter, bool) {
	if filters == nil {
		return nil, false
	}

	filter, ok := filters.fields[path]
	return filter, ok
}

func (j *Marshaller) AsOutputPath(fieldPath string) (string, error) {
	index, ok := j._marshallersInput[fieldPath]
	if !ok {
		return "", fmt.Errorf("not found path %v", fieldPath)
	}

	return j.marshallers[index].outputPath, nil
}

func (j *Marshaller) marshalerByName(name string) (*fieldMarshaller, bool) {
	index, ok := j._marshallersInput[name]
	if !ok {
		return nil, false
	}

	return j.marshallers[index], true
}

func elem(rType reflect.Type) reflect.Type {
	for {
		switch rType.Kind() {
		case reflect.Ptr, reflect.Slice:
			rType = rType.Elem()
		default:
			return rType
		}
	}
}
