package json

import (
	"fmt"
	"github.com/viant/datly/data"
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

var timeType = reflect.TypeOf(time.Time{})

type (
	Marshaller struct {
		rType        reflect.Type
		stringifiers []*fieldStringifier
		config       marshal.Default
	}

	fieldStringifier struct {
		rType  reflect.Type
		xField *xunsafe.Field
		strFn  func(unsafe.Pointer, *strings.Builder) error
		fields []*fieldStringifier

		emptyChecker func(value interface{}) bool
		fieldName    string
		omitEmpty    bool

		isComparable bool
		nilValue     interface{}
		zeroValue    interface{}
	}

	StructMetadata map[string]*FieldMetadata
	FieldMetadata  struct {
		OmitEmpty  bool
		CaseFormat data.CaseFormat
		StructMetadata
	}
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
	stringifiers, err := structStringifiers(j.rType, j.config)
	if err != nil {
		return err
	}

	j.stringifiers = stringifiers
	return nil
}

func structStringifiers(rType reflect.Type, config marshal.Default) ([]*fieldStringifier, error) {
	elem := shared.Elem(rType)
	numField := elem.NumField()

	stringifiers := make([]*fieldStringifier, 0)
	for i := 0; i < numField; i++ {
		stringifier, err := newStringifier(elem.Field(i), config)
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

func newStringifier(field reflect.StructField, config marshal.Default) (*fieldStringifier, error) {
	tag := Parse(field.Tag.Get(TagName))
	if tag.FieldName == "-" {
		return nil, nil
	}

	fieldName := field.Name
	if fieldName[0] > 'Z' || fieldName[0] < 'A' && tag.FieldName == "" {
		return nil, nil
	}

	if tag.FieldName != "" {
		fieldName = tag.FieldName
	} else if config.CaseFormat != 0 {
		fieldName = defaultCaser.Format(fieldName, config.CaseFormat)
	}

	stringifier := &fieldStringifier{
		rType:     field.Type,
		fieldName: fieldName,
		xField:    xunsafe.NewField(field),
		omitEmpty: tag.OmitEmpty || config.OmitEmpty,
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
			updateNonPrimitiveStringifier(f)
			stringifiers, err := structStringifiers(rType, config)
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
		f.strFn = func(ptr unsafe.Pointer, sb *strings.Builder) error {
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

	f.strFn = func(ptr unsafe.Pointer, sb *strings.Builder) error {
		sb.WriteByte('"')
		sb.WriteString(f.xField.Time(ptr).Format(time.RFC3339))
		sb.WriteByte('"')
		return nil
	}
}

func updateNonPrimitiveStringifier(stringifier *fieldStringifier) {
	rType := stringifier.xField.Type
	stringifier.strFn = func(pointer unsafe.Pointer, sb *strings.Builder) error {
		return stringifyNonPrimitive(rType, xunsafe.AsPointer(stringifier.xField.Value(pointer)), stringifier.fields, sb)
	}
	return
}

func updateFloat32Stringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.strFn = func(ptr unsafe.Pointer, sb *strings.Builder) error {
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

	stringifier.strFn = func(ptr unsafe.Pointer, sb *strings.Builder) error {
		stringifyFloat(sb, float64(stringifier.xField.Float32(ptr)))
		return nil
	}
}

func updateFloat64Stringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.strFn = func(ptr unsafe.Pointer, sb *strings.Builder) error {
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

	stringifier.strFn = func(ptr unsafe.Pointer, sb *strings.Builder) error {
		stringifyFloat(sb, stringifier.xField.Float64(ptr))
		return nil
	}
}

func stringifyFloat(sb *strings.Builder, f float64) {
	sb.WriteString(strconv.FormatFloat(f, 'f', -1, 64))
}

func updateStringStringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.strFn = func(ptr unsafe.Pointer, sb *strings.Builder) error {
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

	stringifier.strFn = func(ptr unsafe.Pointer, sb *strings.Builder) error {
		stringifyString(sb, stringifier.xField.String(ptr))
		return nil
	}
}

func stringifyString(sb *strings.Builder, asString string) {
	//TODO: revise for unicode
	sb.WriteByte('"')
	sb.WriteString(strings.ReplaceAll(strings.ReplaceAll(asString, `\`, `\\`), `"`, `\"`))
	sb.WriteByte('"')
}

func updateBoolStringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.strFn = func(pointer unsafe.Pointer, sb *strings.Builder) error {
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

	stringifier.strFn = func(ptr unsafe.Pointer, sb *strings.Builder) error {
		stringifyBool(stringifier.xField.Bool(ptr), sb)
		return nil
	}
}

func stringifyBool(b bool, sb *strings.Builder) {
	if b == true {
		sb.WriteString(`true`)
	} else {
		sb.WriteString(`false`)
	}
}

func updateUint64Stringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.strFn = func(pointer unsafe.Pointer, sb *strings.Builder) error {
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

	stringifier.strFn = func(ptr unsafe.Pointer, sb *strings.Builder) error {
		sb.WriteString(strconv.Itoa(int(stringifier.xField.Uint64(ptr))))
		return nil
	}
}

func updateUint32Stringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.strFn = func(pointer unsafe.Pointer, sb *strings.Builder) error {
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

	stringifier.strFn = func(ptr unsafe.Pointer, sb *strings.Builder) error {
		sb.WriteString(strconv.Itoa(int(stringifier.xField.Uint32(ptr))))
		return nil
	}
}

func updateUint16Stringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.strFn = func(pointer unsafe.Pointer, sb *strings.Builder) error {
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

	stringifier.strFn = func(ptr unsafe.Pointer, sb *strings.Builder) error {
		sb.WriteString(strconv.Itoa(int(stringifier.xField.Uint16(ptr))))
		return nil
	}
}

func updateUint8Stringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.strFn = func(pointer unsafe.Pointer, sb *strings.Builder) error {
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

	stringifier.strFn = func(ptr unsafe.Pointer, sb *strings.Builder) error {
		sb.WriteString(strconv.Itoa(int(stringifier.xField.Uint8(ptr))))
		return nil
	}
}

func updateUintStringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.strFn = func(pointer unsafe.Pointer, sb *strings.Builder) error {
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

	stringifier.strFn = func(ptr unsafe.Pointer, sb *strings.Builder) error {
		sb.WriteString(strconv.Itoa(int(stringifier.xField.Uint(ptr))))
		return nil
	}
}

func updateInt64Stringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.strFn = func(pointer unsafe.Pointer, sb *strings.Builder) error {
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

	stringifier.strFn = func(ptr unsafe.Pointer, sb *strings.Builder) error {
		sb.WriteString(strconv.Itoa(int(stringifier.xField.Int64(ptr))))
		return nil
	}
}

func updateInt32Stringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.strFn = func(pointer unsafe.Pointer, sb *strings.Builder) error {
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

	stringifier.strFn = func(ptr unsafe.Pointer, sb *strings.Builder) error {
		sb.WriteString(strconv.Itoa(int(stringifier.xField.Int32(ptr))))
		return nil
	}
}

func updateInt16Stringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.strFn = func(pointer unsafe.Pointer, sb *strings.Builder) error {
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

	stringifier.strFn = func(ptr unsafe.Pointer, sb *strings.Builder) error {
		sb.WriteString(strconv.Itoa(int(stringifier.xField.Int16(ptr))))
		return nil
	}
}

func updateInt8Stringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.strFn = func(pointer unsafe.Pointer, sb *strings.Builder) error {
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

	stringifier.strFn = func(ptr unsafe.Pointer, sb *strings.Builder) error {
		sb.WriteString(strconv.Itoa(int(stringifier.xField.Int8(ptr))))
		return nil
	}
}

func updateIntStringifier(stringifier *fieldStringifier, wasPtr bool) {
	if wasPtr {
		stringifier.strFn = func(pointer unsafe.Pointer, sb *strings.Builder) error {
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

	stringifier.strFn = func(ptr unsafe.Pointer, sb *strings.Builder) error {
		sb.WriteString(strconv.Itoa(stringifier.xField.Int(ptr)))
		return nil
	}
}

func asObject(ptr unsafe.Pointer, fields []*fieldStringifier, sb *strings.Builder) error {
	if ptr == nil {
		sb.WriteString(null)
		return nil
	}

	counter := 0
	sb.WriteByte('{')
	for _, stringifier := range fields {
		value := stringifier.xField.Value(ptr)
		var isZeroValue, isNil bool
		if stringifier.isComparable {
			isZeroValue = stringifier.zeroValue == value
			isNil = stringifier.nilValue == value
		} else {
			t := stringifier.xField.Type
			valuePtr := stringifier.xField.Pointer(ptr)
			switch t.Kind() {
			case reflect.Ptr:
				isNil = valuePtr == nil
			case reflect.Slice:
				s := (*reflect.SliceHeader)(valuePtr)
				isZeroValue = s != nil && s.Len == 0
				isNil = s != nil && s.Data == 0
			}
			//isZeroValue, isNil = rValue.IsZero(), (rValue.Kind() == reflect.Ptr || rValue.Kind() == reflect.Slice) && rValue.IsNil()
		}

		if stringifier.omitEmpty && isZeroValue {
			continue
		}

		if counter > 0 {
			sb.WriteByte(',')
		}

		sb.WriteByte('"')
		sb.WriteString(stringifier.fieldName)
		sb.WriteString(`":`)
		if isNil {
			sb.WriteString(null)
		} else {
			if err := stringifier.strFn(ptr, sb); err != nil {
				return err
			}
		}

		counter++
	}

	sb.WriteByte('}')
	return nil
}

func (j *Marshaller) Marshal(value interface{}) ([]byte, error) {
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

	sb := &strings.Builder{}
	err := stringifyNonPrimitive(rType, xunsafe.AsPointer(value), j.stringifiers, sb)
	if err != nil {
		return nil, err
	}

	//TODO: reimplement strings.Builder
	return []byte(sb.String()), nil
}

//TODO: remove switch
func stringifyNonPrimitive(rType reflect.Type, ptr unsafe.Pointer, stringifiers []*fieldStringifier, sb *strings.Builder) error {
	switch rType.Kind() {
	case reflect.Struct:
		return asObject(ptr, stringifiers, sb)
	case reflect.Slice:
		return asSlice(rType, ptr, stringifiers, sb)
	default:
		return fmt.Errorf("unsupported type %v", rType)
	}
}

func asSlice(rType reflect.Type, ptr unsafe.Pointer, stringifiers []*fieldStringifier, sb *strings.Builder) error {
	if ptr == nil {
		sb.WriteString(null)
		return nil
	}

	xslice := xunsafe.NewSlice(rType)

	sb.WriteByte('[')
	for i := 0; i < xslice.Len(ptr); i++ {
		if i != 0 {
			sb.WriteByte(',')
		}
		if err := asObject(xunsafe.AsPointer(xslice.ValuePointerAt(ptr, i)), stringifiers, sb); err != nil {
			return err
		}
	}
	sb.WriteByte(']')
	return nil
}
