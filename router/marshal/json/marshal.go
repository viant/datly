package json

import (
	"bytes"
	"fmt"
	"github.com/francoispqt/gojay"
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

var nullBytes = []byte(`null`)

const defaultCaser = format.CaseUpperCamel
const IndexKey = "presenceIndex"

type (
	Marshaller struct {
		rType       reflect.Type
		path        string
		marshallers []*fieldMarshaller
		config      marshal.Default

		cache            sync.Map
		sliceStringifier sync.Map
		unmarshalElem    unmarshallFieldFn
		unmarshalArr     unmarshallFieldFn

		_marshallersOutput map[string]int
		_marshallersInput  map[string]int
		indexUpdater       *presenceUpdater
		inlinable          bool
	}

	fieldMarshaller struct {
		xField    *xunsafe.Field
		marshall  marshallFieldFn
		unmarshal unmarshallFieldFn
		fields    []*fieldMarshaller

		jsonName  string
		fieldName string
		omitEmpty bool
		inline    bool

		path             string
		isComparable     bool
		zeroValue        interface{}
		outputPath       string
		derefCount       int
		indirectAccessor *xunsafe.Field
		derefStart       int
		indexUpdater     *presenceUpdater
		tag              *DefaultTag
	}

	fieldMarshallers  []*fieldMarshaller
	marshallObjFn     func(parentType reflect.Type, ptr unsafe.Pointer, fields []*fieldMarshaller, sb *bytes.Buffer, filters *Filters, path string) error
	marshallFieldFn   func(reflect.Type, unsafe.Pointer, *bytes.Buffer, *Filters) error
	unmarshallFieldFn func(rType reflect.Type, ptr unsafe.Pointer, mainDecoder *gojay.Decoder, nullDecoder *gojay.Decoder) error
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
		path:             initialPath,
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

	elemType := j.rType
	if elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
	}

	if elemType.Kind() == reflect.Slice {
		elemType = elemType.Elem()
	}

	j.unmarshalElem, err = j.unmarshalObject(elemType)
	if err != nil {
		return err
	}

	j.unmarshalArr = j.unmarshalSlice(j.rType)

	return nil
}

func (j *Marshaller) structMarshallers(rType reflect.Type, config marshal.Default, path, outputPath string, dTag *DefaultTag) ([]*fieldMarshaller, error) {
	var wasPtr bool
	if rType.Kind() == reflect.Ptr {
		wasPtr = true
	}

	elemType := shared.Elem(rType)
	if elemType.Kind() != reflect.Struct {
		aField := &fieldMarshaller{
			xField: xunsafe.NewField(reflect.StructField{Name: "TEMP", Type: elemType}),
			tag:    dTag,
		}

		if err := aField.updateFieldMarshaller(rType, config, j, elemType, wasPtr, dTag); err != nil {
			return nil, err
		}

		return []*fieldMarshaller{aField}, nil
	}

	fields := j.groupFields(elemType)
	marshallers, err := j.createStructMarshallers(fields, config, path, outputPath, dTag)
	if err != nil {
		return nil, err
	}

	if len(fields.presenceFields) == 1 {
		updater, err := j.presenceUpdater(fields.presenceFields[0])
		if err != nil {
			return nil, err
		}

		j.indexUpdater = updater
		for _, marshaller := range marshallers {
			marshaller.indexUpdater = updater
		}
	}

	return marshallers, nil
}

func (j *Marshaller) createStructMarshallers(fields *groupedFields, config marshal.Default, path string, outputPath string, tag *DefaultTag) ([]*fieldMarshaller, error) {
	marshallers := make([]*fieldMarshaller, 0)
	if len(fields.inlinable) == 1 {
		j.inlinable = true
		field := fields.inlinable[0]
		marshaller, err := j.NewInlinableMarshaller(field, config, path)
		if err != nil {
			return nil, err
		}

		marshallers = append(marshallers, &fieldMarshaller{
			xField:    xunsafe.NewField(field),
			marshall:  marshaller.MarshallObject,
			unmarshal: marshaller.UnmarshallObject,
			tag:       tag,
		})

	} else {
		for _, field := range fields.regularFields {
			dTag, err := NewDefaultTag(field)
			if err != nil {
				return nil, err
			}

			if err = j.newFieldMarshaller(&marshallers, field, config, path, outputPath, dTag); err != nil {
				return nil, err
			}
		}
	}

	return marshallers, nil
}

func IsPresenceField(field reflect.StructField) bool {
	indexTag := field.Tag.Get(IndexKey)
	return indexTag != ""
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

	tag := ParseXTag(field.Tag.Get(TagName), field.Tag.Get(XTagName))
	if tag.FieldName == "-" {
		return nil
	}

	jsonName := field.Name
	if jsonName[0] > 'Z' || jsonName[0] < 'A' && tag.FieldName == "" {
		return nil
	}

	if tag.FieldName != "" {
		jsonName = tag.FieldName
	} else if config.CaseFormat != 0 {
		jsonName = formatName(jsonName, config.CaseFormat)
	}

	path, outputPath = addToPath(path, field.Name), addToPath(outputPath, jsonName)

	xField := xunsafe.NewField(field)
	marshaller := &fieldMarshaller{
		jsonName:   jsonName,
		fieldName:  field.Name,
		xField:     xField,
		omitEmpty:  tag.OmitEmpty || config.OmitEmpty,
		inline:     tag.Inline,
		path:       path,
		outputPath: outputPath,
		tag:        defaultTag,
	}

	if err := marshaller.init(field, config, j); err != nil {
		return err
	}
	*marshallers = append(*marshallers, marshaller)

	return nil
}

func formatName(jsonName string, caseFormat format.Case) string {
	if jsonName == "ID" {
		switch caseFormat {
		case format.CaseLowerUnderscore, format.CaseLower, format.CaseLowerCamel:
			return "id"
		case format.CaseUpperCamel, format.CaseUpper, format.CaseUpperUnderscore:
			return "ID"
		}
	}

	jsonName = defaultCaser.Format(jsonName, caseFormat)
	return jsonName
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
	case reflect.Struct:
		if rType != timeType {
			marshallers, err := j.structMarshallers(rType, config, f.path, f.outputPath, defaultTag)
			if err != nil {
				return err
			}
			f.fields = marshallers
		}
	}

	marshaler, unmarshaler, err := getMarshalFunctions(parentType, config, j, defaultTag, f.path)
	if err != nil {
		return err
	}

	if marshaler != nil {
		f.marshall = marshaler
	}

	if unmarshaler != nil {
		f.unmarshal = unmarshaler
	}

	return nil
}

func getMarshalFunctions(rType reflect.Type, config marshal.Default, j *Marshaller, defaultTag *DefaultTag, path string) (marshallFieldFn, unmarshallFieldFn, error) {
	switch rType.Kind() {
	case reflect.Int:
		marshaller, unmarshaller := intMarshaller(defaultTag)
		return marshaller, unmarshaller, nil
	case reflect.Int8:
		marshaller, unmarshaller := int8Marshaller(defaultTag)
		return marshaller, unmarshaller, nil
	case reflect.Int16:
		marshaller, unmarshaller := int16Marshaller(defaultTag)
		return marshaller, unmarshaller, nil
	case reflect.Int32:
		marshaller, unmarshaller := int32Marshaller(defaultTag)
		return marshaller, unmarshaller, nil
	case reflect.Int64:
		marshaller, unmarshaller := int64Marshaller(defaultTag)
		return marshaller, unmarshaller, nil
	case reflect.Uint:
		marshaller, unmarshaller := uintMarshaller(defaultTag)
		return marshaller, unmarshaller, nil
	case reflect.Uint8:
		marshaller, unmarshaller := uint8Marshaller(defaultTag)
		return marshaller, unmarshaller, nil
	case reflect.Uint16:
		marshaller, unmarshaller := uint16Marshaller(defaultTag)
		return marshaller, unmarshaller, nil
	case reflect.Uint32:
		marshaller, unmarshaller := uint32Marshaller(defaultTag)
		return marshaller, unmarshaller, nil
	case reflect.Uint64:
		marshaller, unmarshaller := uint64Marshaller(defaultTag)
		return marshaller, unmarshaller, nil
	case reflect.Bool:
		marshaller, unmarshaller := boolMarshaller(defaultTag)
		return marshaller, unmarshaller, nil
	case reflect.String:
		marshaller, unmarshaller := stringMarshaller(defaultTag)
		return marshaller, unmarshaller, nil
	case reflect.Float64:
		marshaller, unmarshaller := float64Marshaller(defaultTag)
		return marshaller, unmarshaller, nil
	case reflect.Float32:
		marshaller, unmarshaller := float32Marshaller(defaultTag)
		return marshaller, unmarshaller, nil
	case reflect.Slice:
		if rType.Kind() == reflect.Slice && rType.Elem().Kind() == reflect.Uint8 {
			marshaller, unmarshaller := bytesMarshaller(defaultTag)
			return marshaller, unmarshaller, nil
		}

	case reflect.Struct:
		if rType == timeType {
			marshaller, unmarshaller := timeMarshaller(defaultTag, config)
			return marshaller, unmarshaller, nil
		}

	case reflect.Interface:
		marshaller, unmarshaller := j.storeOrLoadMarshaller(config, path)
		return marshaller, unmarshaller, nil

	case reflect.Ptr:
		marshaller, unmarshaller, err := getMarshalFunctions(rType.Elem(), config, j, defaultTag, path)
		if err != nil {
			return nil, nil, err
		}

		defaultAppend := null
		if defaultTag._value != nil {
			switch actual := defaultTag._value.(type) {
			case time.Time:
				timeFormat := defaultTag.Format
				if timeFormat == "" {
					timeFormat = time.RFC3339
				}

				defaultAppend = strconv.Quote(actual.Format(timeFormat))
			case string:
				defaultAppend = strconv.Quote(actual)
			default:
				defaultAppend = defaultTag.Value
			}
		}

		return func(r reflect.Type, pointer unsafe.Pointer, buffer *bytes.Buffer, filters *Filters) error {
				if pointer == nil {
					buffer.WriteString(defaultAppend)
					return nil
				}

				pointer = xunsafe.DerefPointer(pointer)
				if pointer == nil {
					buffer.WriteString(defaultAppend)
					return nil
				}

				return marshaller(r.Elem(), pointer, buffer, filters)
			}, func(r reflect.Type, pointer unsafe.Pointer, g *gojay.Decoder, nullDecoder *gojay.Decoder) error {
				if pointer == nil {
					return nil
				}

				if nullDecoder == nil {
					embeddedJSON := &gojay.EmbeddedJSON{}
					if err := g.EmbeddedJSON(embeddedJSON); err != nil {
						return err
					}

					if bytes.Equal(*embeddedJSON, nullBytes) {
						return nil
					}

					nullDecoder = gojay.NewDecoder(bytes.NewReader(*embeddedJSON))
				}

				return unmarshaller(rType, xunsafe.SafeDerefPointer(pointer, r), nullDecoder, nullDecoder)
			}, nil
	}

	switch rType.Kind() {
	case reflect.Slice:
		return j.sliceMarshaller(rType, path)
	case reflect.Struct:
		return j.structMarshaller(rType, path)
	case reflect.Map:
		return j.mapMarshaller(rType, defaultTag, path)
	}

	return nil, nil, fmt.Errorf("unsupported type %v", rType.String())
}

func (j *Marshaller) storeOrLoadMarshaller(config marshal.Default, path string) (marshallFieldFn, unmarshallFieldFn) {
	return func(interfaceType reflect.Type, ptr unsafe.Pointer, buffer *bytes.Buffer, filters *Filters) error {
			marshaller, ok := j.cache.Load(interfaceType)
			if ok {
				return marshaller.(*Marshaller).stringifyValue(interfaceType, ptr, filters, interfaceType, buffer, path)
			}

			interfaceMarshaller, err := newMarshaller(interfaceType, config, path)
			if err != nil {
				return err
			}

			j.cache.Store(interfaceType, interfaceMarshaller)
			return interfaceMarshaller.stringifyValue(interfaceType, ptr, filters, interfaceType, buffer, path)
		}, func(r reflect.Type, pointer unsafe.Pointer, g *gojay.Decoder, nullDecoder *gojay.Decoder) error {
			iface := Interface(GetXType(r), pointer)
			return g.Interface(&iface)
		}
}

func timeMarshaller(tag *DefaultTag, config marshal.Default) (marshallFieldFn, unmarshallFieldFn) {
	timeFormat := time.RFC3339
	if tag.Format != "" {
		timeFormat = tag.Format
	}

	if config.DateLayout != "" {
		timeFormat = config.DateLayout
	}

	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			aTime := xunsafe.AsTime(ptr)
			return appendTime(sb, &aTime, tag, timeFormat)
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
			aTime := xunsafe.AsTimePtr(pointer)
			if err := decoder.AddTime(aTime, timeFormat); err != nil {
				return err
			}
			return nil
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

func (j *Marshaller) sliceMarshaller(sliceType reflect.Type, path string) (marshallFieldFn, unmarshallFieldFn, error) {
	elemType := sliceType.Elem()
	objMarshaller, err := newMarshaller(elemType, j.config, path)
	if err != nil {
		return nil, nil, err
	}

	marshaller, err := objMarshaller.storeOrLoadSliceMarshaller(elemType)
	if err != nil {
		return nil, nil, err
	}

	if sliceType.Kind() == reflect.Ptr {
		sliceType = sliceType.Elem()
	}

	xSlice := xunsafe.NewSlice(sliceType)
	xType := GetXType(elemType)
	return func(r reflect.Type, pointer unsafe.Pointer, buffer *bytes.Buffer, filters *Filters) error {
			return marshaller(r, pointer, objMarshaller.marshallers, buffer, filters, objMarshaller.path)
		}, func(r reflect.Type, pointer unsafe.Pointer, g *gojay.Decoder, nullDecoder *gojay.Decoder) error {
			return g.DecodeArray(newSliceDecoder(r, pointer, xSlice, objMarshaller.unmarshalElem, xType))
		}, nil
}

func (j *Marshaller) structMarshaller(parentType reflect.Type, path string) (marshallFieldFn, unmarshallFieldFn, error) {
	objMarshaller, err := newMarshaller(parentType, j.config, path)
	if err != nil {
		return nil, nil, err
	}

	return func(r reflect.Type, pointer unsafe.Pointer, buffer *bytes.Buffer, filters *Filters) error {
		return objMarshaller.marshalObject(r, pointer, objMarshaller.marshallers, buffer, filters, objMarshaller.path)
	}, objMarshaller.unmarshalElem, nil
}

func float32Marshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			return appendFloat(sb, float64(xunsafe.AsFloat32(ptr)), false, tag)
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
			return decoder.AddFloat32((*float32)(pointer))
		}
}

func float64Marshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			return appendFloat(sb, xunsafe.AsFloat64(ptr), false, tag)
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
			return decoder.AddFloat64(xunsafe.AsFloat64Ptr(pointer))
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

func stringMarshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			aString := xunsafe.AsString(ptr)
			if aString == "" && tag._value != nil {
				aString = tag._value.(string)
			}

			marshallString(sb, aString)
			return nil
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
			return decoder.AddString((xunsafe.AsStringPtr(pointer)))
		}
}

func bytesMarshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			aString := xunsafe.AsString(ptr)
			if aString == "" && tag._value != nil {
				aString = tag._value.(string)
			}

			marshallString(sb, aString)
			return nil
		}, func(r reflect.Type, pointer unsafe.Pointer, g *gojay.Decoder, nullDecoder *gojay.Decoder) error {
			return g.DecodeArray(&BytesSlice{b: (*[]byte)(pointer)})
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

func boolMarshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			marshallBool(xunsafe.AsBool(ptr), sb)
			return nil
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
			return decoder.AddBool(xunsafe.AsBoolPtr(pointer))
		}
}

func marshallBool(b bool, sb *bytes.Buffer) {
	if b {
		sb.WriteString(`true`)
	} else {
		sb.WriteString(`false`)
	}
}

func uint64Marshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			return appendInt(int(xunsafe.AsUint64(ptr)), false, tag, sb)
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
			return decoder.AddUint64(xunsafe.AsUint64Ptr(pointer))
		}
}

func uint32Marshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			return appendInt(int(xunsafe.AsUint32(ptr)), false, tag, sb)
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
			return decoder.AddUint32((*uint32)(pointer))
		}
}

func uint16Marshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			return appendInt(int(xunsafe.AsUint16(ptr)), false, tag, sb)
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
			return decoder.AddUint16((*uint16)(pointer))
		}
}

func uint8Marshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			return appendInt(int(xunsafe.AsUint8(ptr)), false, tag, sb)
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
			return decoder.AddUint8((*uint8)(pointer))
		}
}

func uintMarshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			return appendInt(int(xunsafe.AsUint(ptr)), false, tag, sb)
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
			return decoder.AddUint64((*uint64)((pointer)))
		}
}

func int64Marshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			return appendInt(int(xunsafe.AsInt64(ptr)), false, tag, sb)
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
			return decoder.AddInt64((*int64)((pointer)))
		}
}

func int32Marshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			return appendInt(int(xunsafe.AsInt32(ptr)), false, tag, sb)
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
			return decoder.AddInt32((*int32)((pointer)))
		}
}

func int16Marshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			return appendInt(int(xunsafe.AsInt16(ptr)), false, tag, sb)
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
			return decoder.AddInt16((*int16)((pointer)))
		}
}

func int8Marshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			return appendInt(int(xunsafe.AsInt8(ptr)), false, tag, sb)
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
			return decoder.AddInt8((*int8)((pointer)))
		}
}

func intMarshaller(tag *DefaultTag) (marshallFieldFn, unmarshallFieldFn) {
	return func(parentType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, _ *Filters) error {
			return appendInt(xunsafe.AsInt(ptr), false, tag, sb)
		}, func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
			return decoder.AddInt(xunsafe.AsIntPtr((pointer)))
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

func (j *Marshaller) marshalObject(p reflect.Type, ptr unsafe.Pointer, fields []*fieldMarshaller, sb *bytes.Buffer, filters *Filters, path string) error {
	if ptr == nil {
		sb.WriteString(null)
		return nil
	}

	if j.inlinable {
		return j.marshallers[0].marshall(p, ptr, sb, filters)
	}

	filter, _ := filterByPath(filters, path)
	sb.WriteByte('{')
	prevLen := sb.Len()
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

		if prevLen != sb.Len() {
			sb.WriteByte(',')
		}

		if !stringifier.tag.Embedded {
			sb.WriteByte('"')
			sb.WriteString(stringifier.jsonName)
			sb.WriteString(`":`)
		}

		rType := stringifier.xField.Type

		prevLen = sb.Len()
		if rType.Kind() == reflect.Interface {
			if valuePtr, ok := value.(*interface{}); ok {
				value = *valuePtr
			}
			rType = reflect.TypeOf(value)
			if err := stringifier.marshall(rType, xunsafe.AsPointer(value), sb, filters); err != nil {
				return err
			}
		} else {
			if err := stringifier.marshall(rType, stringifier.xField.Pointer(objPtr), sb, filters); err != nil {
				return err
			}
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
		return j.marshalObject, nil
	case reflect.Slice:
		return j.storeOrLoadSliceMarshaller(rType.Elem())
	case reflect.Interface:
		xType := GetXType(rType)
		return func(parentType reflect.Type, ptr unsafe.Pointer, fields []*fieldMarshaller, sb *bytes.Buffer, filters *Filters, path string) error {
			value := Interface(xType, ptr)
			marshaller, _ := j.storeOrLoadMarshaller(j.config, path)
			return marshaller(reflect.TypeOf(value), xunsafe.AsPointer(value), sb, filters)
		}, nil
	}

	if len(j.marshallers) == 1 {
		return func(parentType reflect.Type, ptr unsafe.Pointer, fields []*fieldMarshaller, sb *bytes.Buffer, filters *Filters, path string) error {
			return j.marshallers[0].marshall(parentType, ptr, sb, filters)
		}, nil
	}

	return nil, fmt.Errorf("can't marshal %v", rType.String())
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
	rType = deref(rType)

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

				if err := j.marshalObject(parentType, xunsafe.AsPointer(xslice.ValueAt(ptr, i)), fields, sb, filters, path); err != nil {
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

			var itemValue interface{}
			if xslice.Type.Elem().Kind() == reflect.Interface {
				itemValue = at
			} else {
				ifaceType = xslice.Type.Elem()
				itemValue = xslice.ValuePointerAt(ptr, i)
			}

			if err = fields[0].marshall(ifaceType, xunsafe.AsPointer(itemValue), sb, filters); err != nil {
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
	if ok {
		return j.marshallers[index], true
	}

	for _, marshaller := range j.marshallers {
		if strings.EqualFold(marshaller.jsonName, name) {
			return marshaller, true
		}
	}

	return nil, false
}

func (j *Marshaller) unmarshalObject(rType reflect.Type) (unmarshallFieldFn, error) {
	parentType := rType
	if rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}

	if rType.Kind() == reflect.Struct && rType != timeType {
		xType := GetXType(rType)

		return func(r reflect.Type, pointer unsafe.Pointer, g *gojay.Decoder, nullDecoder *gojay.Decoder) error {
			return g.Object(j.newStructDecoder(j.path, pointer, xType))
		}, nil
	}

	_, unmarshal, err := getMarshalFunctions(parentType, j.config, j, &DefaultTag{}, j.path)
	return unmarshal, err
}

func (j *Marshaller) unmarshalSlice(rType reflect.Type) unmarshallFieldFn {
	sliceType := rType
	if rType.Kind() == reflect.Ptr {
		sliceType = rType.Elem()
	}

	xslice := xunsafe.NewSlice(sliceType)
	xType := xunsafe.NewType(rType)
	return func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
		return decoder.Array(newSliceDecoder(rType.Elem(), pointer, xslice, j.unmarshalElem, xType))
	}
}

func (j *Marshaller) genericUnmarshaller(rType reflect.Type) unmarshallFieldFn {
	xType := GetXType(rType)
	return func(r reflect.Type, pointer unsafe.Pointer, decoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
		iface := Interface(xType, pointer)
		return decoder.AddInterface(&iface)
	}
}

func (j *Marshaller) mapMarshaller(rType reflect.Type, tag *DefaultTag, path string) (marshallFieldFn, unmarshallFieldFn, error) {
	key := rType.Key()
	value := rType.Elem()

	if key.Kind() == reflect.String && value.Kind() == reflect.Interface {
		isEmbedded := tag.Embedded

		return func(r reflect.Type, pointer unsafe.Pointer, buffer *bytes.Buffer, filters *Filters) error {
				mapPtr := (*map[string]interface{})(pointer)
				if mapPtr == nil {
					return nil
				}

				if !isEmbedded {
					buffer.WriteString("{")
				}

				aMap := *mapPtr
				counter := 0
				for aKey, aValue := range aMap {
					if counter > 0 {
						buffer.WriteString(",")
					}
					counter++
					marshallerFn, _ := j.storeOrLoadMarshaller(j.config, path)
					buffer.WriteString(`"`)
					buffer.WriteString(namesCaseIndex.FormatTo(aKey, j.config.CaseFormat))
					buffer.WriteString(`":`)
					if err := marshallerFn(reflect.TypeOf(aValue), xunsafe.AsPointer(aValue), buffer, filters); err != nil {
						return err
					}

				}

				if !isEmbedded {
					buffer.WriteString("}")
				}

				return nil
			}, func(rType reflect.Type, ptr unsafe.Pointer, mainDecoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
				return fmt.Errorf("unsupported unmarshall dest %v", rType.String())
			}, nil
	}

	return nil, nil, fmt.Errorf("unsupported map type %v", rType.String())
}

func (j *Marshaller) groupFields(elemType reflect.Type) *groupedFields {
	result := &groupedFields{}
	numField := elemType.NumField()

	for i := 0; i < numField; i++ {
		structField := elemType.Field(i)
		xTag := ParseXTag("", structField.Tag.Get(XTagName))
		isRegularField := true

		if xTag.Inline {
			isRegularField = false
			result.inlinable = append(result.inlinable, structField)
		}

		if structField.Tag.Get(IndexKey) != "" {
			isRegularField = false
			result.presenceFields = append(result.presenceFields, structField)
		}

		if isRegularField {
			result.regularFields = append(result.regularFields, structField)
		}
	}

	return result
}

func Interface(xType *xunsafe.Type, pointer unsafe.Pointer) interface{} {
	if xType.Kind() == reflect.Interface {
		return xunsafe.AsInterface(pointer)
	}

	return xType.Interface(pointer)
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
