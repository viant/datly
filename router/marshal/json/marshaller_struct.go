package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
	"unsafe"
)

type (
	StructMarshaller struct {
		rType               reflect.Type
		inlinableMarshaller *InlinableMarshaller
		indexUpdater        *PresenceUpdater
		marshallersIndex    map[string]int
		marshallers         []*MarshallerWithField
		config              marshal.Default

		path       string
		outputPath string

		cache *Cache
		xType *xunsafe.Type
	}

	MarshallerWithField struct {
		marshaller     Marshaler
		xField         *xunsafe.Field
		indirectXField *xunsafe.Field //in case anonymous pointer field
		tag            *DefaultTag
		indexUpdater   *PresenceUpdater
		MarshallerMetadata
	}

	MarshallerMetadata struct {
		fieldName  string
		jsonName   string
		path       string
		comparable bool
		zeroValue  interface{}
		outputPath string
		omitEmpty  bool
	}

	decoder struct {
		ptr        unsafe.Pointer
		path       string
		xType      *xunsafe.Type
		marshaller *StructMarshaller
	}
)

func NewStructMarshaller(config marshal.Default, rType reflect.Type, path string, outputPath string, dTag *DefaultTag, cache *Cache) (*StructMarshaller, error) {
	result := &StructMarshaller{
		path:             path,
		outputPath:       outputPath,
		xType:            GetXType(rType),
		rType:            rType,
		config:           config,
		cache:            cache,
		marshallersIndex: map[string]int{},
	}

	return result, result.init()
}

func (s *StructMarshaller) UnmarshallObject(pointer unsafe.Pointer, mainDecoder *gojay.Decoder, _ *gojay.Decoder) error {
	if s.indexUpdater != nil {
		indexPtr := s.indexUpdater.xField.ValuePointer(pointer)
		if indexPtr == nil {
			var rValue reflect.Value
			if s.indexUpdater.xField.Type.Kind() == reflect.Ptr {
				rValue = reflect.New(s.indexUpdater.xField.Type.Elem())
			} else {
				rValue = reflect.New(s.indexUpdater.xField.Type)
			}

			iface := rValue.Interface()
			s.indexUpdater.xField.SetValue(pointer, iface)
		}
	}

	d := &decoder{
		marshaller: s,
		xType:      s.xType,
		ptr:        pointer,
		path:       s.path,
	}

	return mainDecoder.DecodeObject(d)
}

func (s *StructMarshaller) MarshallObject(ptr unsafe.Pointer, sb *Session) error {
	if ptr == nil {
		sb.WriteString(null)
		return nil
	}

	if s.inlinableMarshaller != nil {
		return s.inlinableMarshaller.MarshallObject(ptr, sb)
	}

	filter, _ := filterByPath(sb.Filters, s.path)
	sb.WriteByte('{')
	prevLen := sb.Len()
	for _, stringifier := range s.marshallers {
		if isExcluded(filter, stringifier.fieldName, s.config, stringifier.path) {
			continue
		}

		objPtr := ptr
		if stringifier.indirectXField != nil {
			objPtr = stringifier.indirectXField.ValuePointer(objPtr)
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

		prevLen = sb.Len()
		if err := stringifier.marshaller.MarshallObject(stringifier.xField.Pointer(objPtr), sb); err != nil {
			return err
		}
	}

	sb.WriteByte('}')
	return nil
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

func (s *StructMarshaller) init() error {
	fields := groupFields(s.rType)
	marshallers, err := s.createStructMarshallers(fields, s.path, s.outputPath, &DefaultTag{})
	if err != nil {
		return err
	}

	if len(fields.presenceFields) == 1 {
		updater, err := NewPresenceUpdater(fields.presenceFields[0])
		if err != nil {
			return err
		}

		s.indexUpdater = updater
		for _, marshaller := range marshallers {
			marshaller.indexUpdater = updater
		}
	}

	for i, marshaller := range marshallers {
		s.marshallersIndex[marshaller.jsonName] = i
		s.marshallersIndex[strings.ToLower(marshaller.jsonName)] = i
	}

	s.marshallers = marshallers

	return nil
}

func (s *StructMarshaller) createStructMarshallers(fields *groupedFields, path string, outputPath string, dTag *DefaultTag) ([]*MarshallerWithField, error) {
	marshallers := make([]*MarshallerWithField, 0)
	if len(fields.inlinable) == 1 {
		field := fields.inlinable[0]
		marshaller, err := NewInlinableMarshaller(field, s.config, path, outputPath, dTag, s.cache)
		if err != nil {
			return nil, err
		}

		s.inlinableMarshaller = marshaller
	} else {
		for _, field := range fields.regularFields {
			dTag, err := NewDefaultTag(field)
			if err != nil {
				return nil, err
			}

			if err = s.newFieldMarshaller(&marshallers, field, path, outputPath, dTag); err != nil {
				return nil, err
			}
		}
	}

	return marshallers, nil
}

func (s *StructMarshaller) newFieldMarshaller(marshallers *[]*MarshallerWithField, field reflect.StructField, path string, outputPath string, dTag *DefaultTag) error {
	if field.Anonymous {
		rType, ptrSize := field.Type, 0
		for rType.Kind() == reflect.Ptr {
			rType = rType.Elem()
			ptrSize++
		}

		anonymousMarshallers, err := s.createStructMarshallers(groupFields(rType), path, outputPath, dTag)
		if err != nil {
			return err
		}

		for _, marshaller := range anonymousMarshallers {
			if ptrSize == 0 {
				marshaller.xField.Offset += field.Offset
			} else {
				marshaller.indirectXField = xunsafe.NewField(field)
			}
		}

		*marshallers = append(*marshallers, anonymousMarshallers...)
		return nil
	}

	tag := ParseXTag(field.Tag.Get(TagName), field.Tag.Get(XTagName))
	if tag.Transient {
		return nil
	}

	jsonName := field.Name
	if jsonName[0] > 'Z' || jsonName[0] < 'A' && tag.FieldName == "" {
		return nil
	}

	if tag.FieldName != "" {
		jsonName = tag.FieldName
	} else if s.config.CaseFormat != 0 {
		jsonName = formatName(jsonName, s.config.CaseFormat)
	}

	path, outputPath = addToPath(path, field.Name), addToPath(outputPath, jsonName)

	xField := xunsafe.NewField(field)
	marshaller := &MarshallerWithField{
		xField: xField,
		tag:    dTag,
		MarshallerMetadata: MarshallerMetadata{
			path:       path,
			outputPath: outputPath,
			omitEmpty:  tag.OmitEmpty || s.config.OmitEmpty,
			jsonName:   jsonName,
			fieldName:  field.Name,
		},
	}

	if err := marshaller.init(field, s.config, s.cache); err != nil {
		return err
	}
	*marshallers = append(*marshallers, marshaller)

	return nil
}

func (s *StructMarshaller) marshallerByName(name string) (*MarshallerWithField, bool) {
	index, ok := s.marshallersIndex[name]

	if ok {
		return s.marshallers[index], true
	}

	index, ok = s.marshallersIndex[strings.ToLower(name)]
	if ok {
		return s.marshallers[index], true
	}

	return nil, false
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

func (f *MarshallerWithField) init(field reflect.StructField, config marshal.Default, cache *Cache) error {
	defaultTag, err := NewDefaultTag(field)
	if err != nil {
		return err
	}

	rType := field.Type
	if field.Type.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}

	f.comparable = rType.Comparable()
	f.zeroValue = reflect.Zero(field.Type).Interface()

	marshaller, err := cache.LoadMarshaller(field.Type, config, f.path, f.outputPath, defaultTag)
	f.marshaller = marshaller
	return err
}

func isZeroValue(ptr unsafe.Pointer, stringifier *MarshallerWithField, value interface{}) bool {
	if stringifier.comparable {
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
	case reflect.Map:
		return reflect.ValueOf(value).Len() == 0
	}

	//this should not happen, all the cases should be covered earlier
	return false
}

func filterByPath(filters *Filters, path string) (Filter, bool) {
	if filters == nil {
		return nil, false
	}

	filter, ok := filters.fields[path]
	return filter, ok
}

func groupFields(elemType reflect.Type) *groupedFields {
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

func (d *decoder) UnmarshalJSONObject(decoder *gojay.Decoder, fieldName string) error {
	marshaller, ok := d.marshaller.marshallerByName(fieldName)
	if !ok {
		return nil
	}

	if err := marshaller.marshaller.UnmarshallObject(marshaller.xField.Pointer(d.ptr), decoder, nil); err != nil {
		return err
	}

	d.updatePresenceIfNeeded(marshaller)
	return nil
}

func (d *decoder) updatePresenceIfNeeded(marshaller *MarshallerWithField) {
	updater := marshaller.indexUpdater
	if updater == nil {
		return
	}

	xField := updater.fields[marshaller.fieldName]
	if xField == nil {
		return
	}

	ptr := updater.xField.ValuePointer(d.ptr)
	xField.SetBool(ptr, true)
}

func (d *decoder) NKeys() int {
	return len(d.marshaller.marshallers)
}
