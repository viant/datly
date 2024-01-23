package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/gateway/router/marshal/config"
	structology "github.com/viant/structology"
	"github.com/viant/tagly/format"
	"github.com/viant/tagly/format/text"
	xunsafe "github.com/viant/xunsafe"
	"reflect"
	"strings"
	"unicode"
	"unsafe"
)

type (
	structMarshaller struct {
		rType               reflect.Type
		inlinableMarshaller *inlinableMarshaller
		indexUpdater        *presenceUpdater
		marshallersIndex    map[string]int
		marshallers         []*marshallerWithField
		config              *config.IOConfig

		path       string
		outputPath string

		cache *marshallersCache
		xType *xunsafe.Type
	}

	marshallerWithField struct {
		marshaller     marshaler
		xField         *xunsafe.Field
		indirectXField *xunsafe.Field //in case anonymous pointer field
		tag            *format.Tag
		indexUpdater   *presenceUpdater
		marshallerMetadata
	}

	marshallerMetadata struct {
		fieldName  string
		jsonName   string
		path       string
		comparable bool
		zeroValue  interface{}
		outputPath string
		omitEmpty  bool
	}

	structDecoder struct {
		ptr        unsafe.Pointer
		path       string
		xType      *xunsafe.Type
		marshaller *structMarshaller
		session    *UnmarshalSession
	}
)

func newStructMarshaller(config *config.IOConfig, rType reflect.Type, path string, outputPath string, dTag *format.Tag, cache *marshallersCache) (*structMarshaller, error) {
	result := &structMarshaller{
		path:             path,
		outputPath:       outputPath,
		xType:            getXType(rType),
		rType:            rType,
		config:           config,
		cache:            cache,
		marshallersIndex: map[string]int{},
	}

	return result, result.init()
}

func (s *structMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
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

	d := &structDecoder{
		marshaller: s,
		xType:      s.xType,
		ptr:        pointer,
		path:       s.path,
		session:    session,
	}

	return decoder.Decode(d)
}

func (s *structMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
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

		isNil := false
		objPtr := ptr
		if stringifier.indirectXField != nil {
			isIgnore := stringifier.indirectXField.Tag.Get("json") == "-"
			if isIgnore {
				continue
			}
			objPtr = stringifier.indirectXField.ValuePointer(objPtr)
			if !stringifier.omitEmpty && strings.Contains(stringifier.indirectXField.Tag.Get("json"), "omitempty") {
				stringifier.omitEmpty = true
			}
			if objPtr == nil {
				isNil = true
			}
		}

		var value interface{}
		isZeroVal := !isNil
		if !isNil {
			if objPtr != nil {
				value = stringifier.xField.Value(objPtr)
			}
			if (objPtr == nil || value == nil) && stringifier.xField.Kind() == reflect.Slice {
				value = reflect.New(stringifier.xField.Type).Interface()
				objPtr = xunsafe.AsPointer(value)
			}
			isZeroVal = isZeroValue(objPtr, stringifier, value)
		}
		if stringifier.omitEmpty && isZeroVal {
			continue
		}
		if prevLen != sb.Len() {
			sb.WriteByte(',')
		}

		if !stringifier.tag.Inline {
			sb.WriteByte('"')
			sb.WriteString(stringifier.jsonName)
			sb.WriteString(`":`)
		}

		if objPtr == nil {
			sb.WriteString(null)
			continue
		}
		fieldValue := stringifier.xField.Pointer(objPtr)
		if fieldValue == nil {
			sb.WriteString(null)
			continue
		}

		prevLen = sb.Len()
		if err := stringifier.marshaller.MarshallObject(fieldValue, sb); err != nil {
			return err
		}
	}

	sb.WriteByte('}')
	return nil
}

func isExcluded(filter Filter, name string, ioConfig *config.IOConfig, path string) bool {
	if ioConfig.Exclude != nil {
		if _, ok := ioConfig.Exclude[path]; ok {
			return true
		}
		normalizedPath := config.NormalizeExclusionKey(path)
		if _, ok := ioConfig.Exclude[normalizedPath]; ok {
			return true
		}
	}
	if filter == nil {
		return false
	}
	_, ok := filter[name]
	return !ok
}

func (s *structMarshaller) init() error {
	fields := groupFields(s.rType)
	marshallers, err := s.createStructMarshallers(fields, s.path, s.outputPath, &format.Tag{})
	if err != nil {
		return err
	}

	if len(fields.presenceFields) == 1 {
		updater, err := newPresenceUpdater(fields.presenceFields[0])
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

func (s *structMarshaller) createStructMarshallers(fields *groupedFields, path string, outputPath string, dTag *format.Tag) ([]*marshallerWithField, error) {
	marshallers := make([]*marshallerWithField, 0)
	if len(fields.inlinable) == 1 {
		field := fields.inlinable[0]
		marshaller, err := newInlinableMarshaller(field, s.config, path, outputPath, dTag, s.cache)
		if err != nil {
			return nil, err
		}

		s.inlinableMarshaller = marshaller
	} else {
		for _, field := range fields.regularFields {
			dTag, err := format.Parse(field.Tag, TagName, XTagName)
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

func (s *structMarshaller) newFieldMarshaller(marshallers *[]*marshallerWithField, field reflect.StructField, path string, outputPath string, dTag *format.Tag) error {
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

	if dTag.Ignore {
		return nil
	}

	jsonName := field.Name
	if !unicode.IsLetter(rune(jsonName[0])) && dTag.Name == "" {
		return nil
	}

	if dTag.Name != "" {
		jsonName = dTag.Name
	} else if dTag.CaseFormat == "-" {
		if dTag.Name != "" {
			jsonName = dTag.Name
		}
	} else if s.config.CaseFormat != "" {
		jsonName = formatName(jsonName, s.config.CaseFormat)
	}
	path, outputPath = addToPath(path, field.Name), addToPath(outputPath, jsonName)

	xField := xunsafe.NewField(field)
	marshaller := &marshallerWithField{
		xField: xField,
		tag:    dTag,
		marshallerMetadata: marshallerMetadata{
			path:       path,
			outputPath: outputPath,
			omitEmpty:  dTag.Omitempty || s.config.OmitEmpty,
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

func (s *structMarshaller) marshallerByName(name string) (*marshallerWithField, bool) {
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

func formatName(jsonName string, caseFormat text.CaseFormat) string {
	//TODO do we still need it
	fromCaseFormat := defaultCaser
	if jsonName == "ID" {
		switch caseFormat {
		case text.CaseFormatLowerUnderscore, text.CaseFormatLower, text.CaseFormatLowerCamel:
			return "id"
		case text.CaseFormatUpperCamel, text.CaseFormatUpper, text.CaseFormatUpperUnderscore:
			return "ID"
		}
	}

	jsonName = fromCaseFormat.Format(jsonName, caseFormat)
	return jsonName
}

func addToPath(path, field string) string {
	if path == "" {
		return field
	}
	return path + "." + field
}

func (f *marshallerWithField) init(field reflect.StructField, config *config.IOConfig, cache *marshallersCache) error {
	defaultTag, err := format.Parse(field.Tag, TagName, XTagName)
	if err != nil {
		return err
	}

	rType := field.Type
	if field.Type.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}

	f.comparable = rType.Comparable()
	f.zeroValue = reflect.Zero(field.Type).Interface()

	marshaller, err := cache.loadMarshaller(field.Type, config, f.path, f.outputPath, defaultTag)
	f.marshaller = marshaller
	return err
}

func isZeroValue(ptr unsafe.Pointer, stringifier *marshallerWithField, value interface{}) bool {
	if stringifier.comparable {
		if value == nil {
			return true
		}
		return stringifier.zeroValue == value
	}

	t := stringifier.xField.Type
	valuePtr := stringifier.xField.Pointer(ptr)
	switch t.Kind() {
	case reflect.Ptr:
		return (*unsafe.Pointer)(valuePtr) == nil || *(*unsafe.Pointer)(valuePtr) == nil
	case reflect.Slice:
		if valuePtr == nil {
			return true
		}
		s := (*reflect.SliceHeader)(valuePtr)
		return s == nil || s.Len == 0
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

		if structology.IsSetMarker(structField.Tag) {
			isRegularField = false
			result.presenceFields = append(result.presenceFields, structField)
		}

		if isRegularField {
			result.regularFields = append(result.regularFields, structField)
		}
	}

	return result
}

func (d *structDecoder) UnmarshalJSONObject(decoder *gojay.Decoder, fieldName string) error {
	marshaller, ok := d.marshaller.marshallerByName(fieldName)
	if !ok {
		return nil
	}

	if len(d.session.PathMarshaller) > 0 {
		interceptor, ok := d.session.PathMarshaller[marshaller.path]
		if ok {
			return interceptor(marshaller.xField.Addr(d.ptr), decoder, d.session.Options...)
		}
	}

	if err := marshaller.marshaller.UnmarshallObject(marshaller.xField.Pointer(d.ptr), decoder, nil, d.session); err != nil {
		return err
	}

	d.updatePresenceIfNeeded(marshaller)
	return nil
}

func (d *structDecoder) updatePresenceIfNeeded(marshaller *marshallerWithField) {
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

func (d *structDecoder) NKeys() int {
	return len(d.marshaller.marshallers)
}
