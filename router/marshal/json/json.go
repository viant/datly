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
	"time"
	"unsafe"
)

const null = `null`
const defaultCaser = format.CaseUpperCamel

type (
	Marshaller struct {
		rType        reflect.Type
		stringifiers []*fieldStringifier
		config       marshal.Default
	}

	fieldStringifier struct {
		xField   *xunsafe.Field
		marshall marshallFieldFn
		fields   []*fieldStringifier

		outputName string
		fieldName  string
		omitEmpty  bool

		path         string
		isComparable bool
		nilValue     interface{}
		zeroValue    interface{}
	}

	marshallObjFn   func(ptr unsafe.Pointer, fields []*fieldStringifier, sb *bytes.Buffer, filters *Filters, path string) error
	marshallFieldFn func(unsafe.Pointer, *bytes.Buffer, *Filters) error
)

func New(rType reflect.Type, config marshal.Default) (*Marshaller, error) {
	json := &Marshaller{
		rType:  rType,
		config: config,
	}

	if err := json.init(); err != nil {
		return nil, err
	}

	return json, nil
}

func (j *Marshaller) init() error {
	stringifiers, err := structStringifiers(j.rType, j.config, "")
	if err != nil {
		return err
	}

	j.stringifiers = stringifiers
	return nil
}

func structStringifiers(rType reflect.Type, config marshal.Default, path string) ([]*fieldStringifier, error) {
	elem := shared.Elem(rType)
	numField := elem.NumField()

	stringifiers := make([]*fieldStringifier, 0)
	for i := 0; i < numField; i++ {
		stringifier, err := newStringifier(elem.Field(i), config, path)
		if err != nil {
			return nil, err
		}

		if stringifier == nil {
			continue
		}

		stringifiers = append(stringifiers, stringifier)
	}

	return stringifiers, nil
}

func newStringifier(field reflect.StructField, config marshal.Default, path string) (*fieldStringifier, error) {
	tag := Parse(field.Tag.Get(TagName))
	if tag.FieldName == "-" {
		return nil, nil
	}

	outputField := field.Name
	if outputField[0] > 'Z' || outputField[0] < 'A' && tag.FieldName == "" {
		return nil, nil
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

	stringifier := &fieldStringifier{
		outputName: outputField,
		fieldName:  field.Name,
		xField:     xunsafe.NewField(field),
		omitEmpty:  tag.OmitEmpty || config.OmitEmpty,
		path:       path,
	}

	if err := stringifier.init(field, config); err != nil {
		return nil, err
	}

	return stringifier, nil
}

func (f *fieldStringifier) init(field reflect.StructField, config marshal.Default) error {
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
		updateIntStringifier(f, wasPtr)
	case reflect.Int8:
		updateInt8Stringifier(f, wasPtr)
	case reflect.Int16:
		updateInt16Stringifier(f, wasPtr)
	case reflect.Int32:
		updateInt32Stringifier(f, wasPtr)
	case reflect.Int64:
		updateInt64Stringifier(f, wasPtr)
	case reflect.Uint:
		updateUintStringifier(f, wasPtr)
	case reflect.Uint8:
		updateUint8Stringifier(f, wasPtr)
	case reflect.Uint16:
		updateUint16Stringifier(f, wasPtr)
	case reflect.Uint32:
		updateUint32Stringifier(f, wasPtr)
	case reflect.Uint64:
		updateUint64Stringifier(f, wasPtr)
	case reflect.Bool:
		updateBoolStringifier(f, wasPtr)
	case reflect.String:
		updateStringStringifier(f, wasPtr)
	case reflect.Float64:
		updateFloat64Stringifier(f, wasPtr)
	case reflect.Float32:
		updateFloat32Stringifier(f, wasPtr)
	case reflect.Slice, reflect.Struct:
		if rType == timeType {
			updateTimeStringifier(f, wasPtr)
		} else {
			if err := updateNonPrimitiveStringifier(f); err != nil {
				return err
			}

			childrenPath := f.fieldName
			if f.path != "" {
				childrenPath = f.path + "." + f.fieldName
			}

			stringifiers, err := structStringifiers(rType, config, childrenPath)
			if err != nil {
				return err
			}
			f.fields = stringifiers
		}

	default:
		return fmt.Errorf("unsupported type %v", field.Type.String())
	}
	return nil
}

func updateTimeStringifier(f *fieldStringifier, wasPtr bool) {
	if wasPtr {
		f.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			timePtr := f.xField.TimePtr(ptr)
			if timePtr == nil {
				sb.WriteString(null)
			} else {
				sb.WriteByte('"')
				sb.WriteString(timePtr.Format(time.RFC3339))
				sb.WriteByte('"')
			}
			return nil
		}
		return
	}

	f.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		sb.WriteByte('"')
		sb.WriteString(f.xField.Time(ptr).Format(time.RFC3339))
		sb.WriteByte('"')
		return nil
	}
}

func updateNonPrimitiveStringifier(stringifier *fieldStringifier) error {
	marshaller, err := stringifyNonPrimitive(stringifier.xField.Type)

	if err != nil {
		return err
	}

	stringifier.marshall = func(pointer unsafe.Pointer, sb *bytes.Buffer, filters *Filters) error {
		return marshaller(xunsafe.AsPointer(stringifier.xField.Value(pointer)), stringifier.fields, sb, filters, stringifier.path)
	}
	return nil
}

func updateFloat32Stringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			float32Ptr := stringifier.xField.Float32Ptr(ptr)
			if float32Ptr == nil {
				sb.WriteString(null)
			} else {
				stringifyFloat(sb, float64(*float32Ptr))
			}
			return nil
		}

		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		stringifyFloat(sb, float64(stringifier.xField.Float32(ptr)))
		return nil
	}
}

func updateFloat64Stringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			floatPtr := stringifier.xField.Float64Ptr(ptr)
			if floatPtr == nil {
				sb.WriteString(null)
			} else {
				stringifyFloat(sb, *floatPtr)
			}
			return nil
		}

		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		stringifyFloat(sb, stringifier.xField.Float64(ptr))
		return nil
	}
}

func stringifyFloat(sb *bytes.Buffer, f float64) {
	sb.WriteString(strconv.FormatFloat(f, 'f', -1, 64))
}

func updateStringStringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			stringPtr := stringifier.xField.StringPtr(ptr)
			if stringPtr == nil {
				sb.WriteString(null)
			} else {
				stringifyString(sb, *stringPtr)
			}

			return nil
		}

		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		stringifyString(sb, stringifier.xField.String(ptr))
		return nil
	}
}

func stringifyString(sb *bytes.Buffer, asString string) {
	//TODO: revise for unicode
	sb.WriteByte('"')
	if strings.Contains(asString, `"`) {
		sb.WriteString(strings.ReplaceAll(strings.ReplaceAll(asString, `\`, `\\`), `"`, `\"`))
	} else {
		sb.WriteString(asString)
	}

	sb.WriteByte('"')
}

func updateBoolStringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.marshall = func(pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			bPtr := stringifier.xField.BoolPtr(pointer)
			if bPtr == nil {
				sb.WriteString(null)
			} else {
				stringifyBool(*bPtr, sb)
			}

			return nil
		}

		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		stringifyBool(stringifier.xField.Bool(ptr), sb)
		return nil
	}
}

func stringifyBool(b bool, sb *bytes.Buffer) {
	if b == true {
		sb.WriteString(`true`)
	} else {
		sb.WriteString(`false`)
	}
}

func updateUint64Stringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.marshall = func(pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.Uint64Ptr(pointer)
			if intPtr != nil {
				sb.WriteString(strconv.Itoa(int(*intPtr)))
			} else {
				sb.WriteString(null)
			}
			return nil
		}
		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		sb.WriteString(strconv.Itoa(int(stringifier.xField.Uint64(ptr))))
		return nil
	}
}

func updateUint32Stringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.marshall = func(pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.Uint32Ptr(pointer)
			if intPtr != nil {
				sb.WriteString(strconv.Itoa(int(*intPtr)))
			} else {
				sb.WriteString(null)
			}
			return nil
		}
		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		sb.WriteString(strconv.Itoa(int(stringifier.xField.Uint32(ptr))))
		return nil
	}
}

func updateUint16Stringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.marshall = func(pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.Uint16Ptr(pointer)
			if intPtr != nil {
				sb.WriteString(strconv.Itoa(int(*intPtr)))
			} else {
				sb.WriteString(null)
			}
			return nil
		}
		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		sb.WriteString(strconv.Itoa(int(stringifier.xField.Uint16(ptr))))
		return nil
	}
}

func updateUint8Stringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.marshall = func(pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.Uint8Ptr(pointer)
			if intPtr != nil {
				sb.WriteString(strconv.Itoa(int(*intPtr)))
			} else {
				sb.WriteString(null)
			}
			return nil
		}
		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		sb.WriteString(strconv.Itoa(int(stringifier.xField.Uint8(ptr))))
		return nil
	}
}

func updateUintStringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.marshall = func(pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.UintPtr(pointer)
			if intPtr != nil {
				sb.WriteString(strconv.Itoa(int(*intPtr)))
			} else {
				sb.WriteString(null)
			}

			return nil
		}
		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		sb.WriteString(strconv.Itoa(int(stringifier.xField.Uint(ptr))))
		return nil
	}
}

func updateInt64Stringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.marshall = func(pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.Int64Ptr(pointer)
			if intPtr != nil {
				sb.WriteString(strconv.Itoa(int(*intPtr)))
			} else {
				sb.WriteString(null)
			}

			return nil
		}
		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		sb.WriteString(strconv.Itoa(int(stringifier.xField.Int64(ptr))))
		return nil
	}
}

func updateInt32Stringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.marshall = func(pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.Int32Ptr(pointer)
			if intPtr != nil {
				sb.WriteString(strconv.Itoa(int(*intPtr)))
			} else {
				sb.WriteString(null)
			}

			return nil
		}
		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		sb.WriteString(strconv.Itoa(int(stringifier.xField.Int32(ptr))))
		return nil
	}
}

func updateInt16Stringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.marshall = func(pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.Int16Ptr(pointer)
			if intPtr != nil {
				sb.WriteString(strconv.Itoa(int(*intPtr)))
			} else {
				sb.WriteString(null)
			}

			return nil
		}
		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		sb.WriteString(strconv.Itoa(int(stringifier.xField.Int16(ptr))))
		return nil
	}
}

func updateInt8Stringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.marshall = func(pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.Int8Ptr(pointer)
			if intPtr != nil {
				sb.WriteString(strconv.Itoa(int(*intPtr)))
			} else {
				sb.WriteString(null)
			}

			return nil
		}
		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		sb.WriteString(strconv.Itoa(int(stringifier.xField.Int8(ptr))))
		return nil
	}
}

func updateIntStringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.marshall = func(pointer unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			intPtr := stringifier.xField.IntPtr(pointer)
			if intPtr != nil {
				sb.WriteString(strconv.Itoa(*intPtr))
			} else {
				sb.WriteString(null)
			}

			return nil
		}
		return
	}

	stringifier.marshall = func(ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
		sb.WriteString(strconv.Itoa(stringifier.xField.Int(ptr)))
		return nil
	}
}

func asObject(ptr unsafe.Pointer, fields []*fieldStringifier, sb *bytes.Buffer, filters *Filters, path string) error {
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
		isZeroValue, isNil := checkValue(ptr, stringifier, value)
		if stringifier.omitEmpty && isZeroValue {
			continue
		}

		if counter > 0 {
			sb.WriteByte(',')
		}

		sb.WriteByte('"')
		sb.WriteString(stringifier.outputName)
		sb.WriteString(`":`)
		if isNil {
			sb.WriteString(null)
		} else {
			if err := stringifier.marshall(ptr, sb, filters); err != nil {
				return err
			}
		}

		counter++
	}

	sb.WriteByte('}')
	return nil
}

func checkValue(ptr unsafe.Pointer, stringifier *fieldStringifier, value interface{}) (isZeroValue bool, isNil bool) {
	if stringifier.isComparable {
		return stringifier.zeroValue == value, stringifier.nilValue == value
	}

	t := stringifier.xField.Type
	valuePtr := stringifier.xField.Pointer(ptr)
	switch t.Kind() {
	case reflect.Ptr:
		return valuePtr == nil, valuePtr == nil
	case reflect.Slice:
		s := (*reflect.SliceHeader)(valuePtr)
		return s != nil && s.Len == 0, s != nil && s.Data == 0
	}

	//this should not happen, all the cases should be covered earlier
	return false, false
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

	sb := bufferPool.Get()
	stringifier, err := stringifyNonPrimitive(rType)
	if err != nil {
		return nil, err
	}

	if err = stringifier(xunsafe.AsPointer(value), j.stringifiers, sb, filters, ""); err != nil {
		return nil, err
	}

	output := make([]byte, len(sb.Bytes()))
	copy(output, sb.Bytes())

	bufferPool.Put(sb)
	return output, nil
}

func stringifyNonPrimitive(rType reflect.Type) (marshallObjFn, error) {
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}

	switch rType.Kind() {
	case reflect.Struct:
		return asObject, nil
	case reflect.Slice:
		return storeOrLoadSliceMarshaller(rType)
	default:
		return nil, fmt.Errorf("unsupported type %v", rType)
	}
}

func storeOrLoadSliceMarshaller(rType reflect.Type) (marshallObjFn, error) {
	encoder, ok := sliceStringifier.Load(rType)
	if ok {
		if marshaler, ok := encoder.(marshallObjFn); ok {
			return marshaler, nil
		}
	}

	marshaler := asSlice(rType)
	sliceStringifier.Store(rType, marshaler)
	return marshaler, nil
}

func asSlice(rType reflect.Type) marshallObjFn {
	xslice := xunsafe.NewSlice(rType)
	return func(ptr unsafe.Pointer, fields []*fieldStringifier, sb *bytes.Buffer, filters *Filters, path string) error {
		if ptr == nil {
			sb.WriteString(null)
			return nil
		}

		sb.WriteByte('[')
		for i := 0; i < xslice.Len(ptr); i++ {
			if i != 0 {
				sb.WriteByte(',')
			}
			if err := asObject(xunsafe.AsPointer(xslice.ValuePointerAt(ptr, i)), fields, sb, filters, path); err != nil {
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
