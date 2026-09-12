package content

import (
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/viant/datly/gateway/router/marshal/config"
	legacyjson "github.com/viant/datly/gateway/router/marshal/json"
	structjson "github.com/viant/structology/encoding/json"
	"github.com/viant/tagly/format"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
)

var DefaultJSONEngineTypeName = reflect.TypeOf(StructologyJSONRuntime{}).PkgPath() + "/" + reflect.TypeOf(StructologyJSONRuntime{}).Name()

func newJSONMarshaller(ioConfig *config.IOConfig, engine string, legacy *legacyjson.Marshaller, lookupType xreflect.LookupType) (JSONMarshallerEngine, JSONUnmarshallerEngine, error) {
	typeName := normalizeJSONEngineTypeName(engine)
	if rType := xunsafe.LookupType(typeName); rType != nil {
		return newJSONMarshallerByReflectType(rType, typeName, ioConfig, legacy)
	}

	if lookupType == nil {
		return nil, nil, fmt.Errorf("unsupported json marshaller engine: %s", typeName)
	}
	return newJSONMarshallerByType(lookupType, typeName, ioConfig, legacy)
}

func normalizeJSONEngineTypeName(engine string) string {
	normalized := strings.TrimSpace(engine)
	if normalized == "" {
		return DefaultJSONEngineTypeName
	}
	return normalized
}

func newJSONMarshallerByType(lookupType xreflect.LookupType, typeName string, ioConfig *config.IOConfig, legacy *legacyjson.Marshaller) (JSONMarshallerEngine, JSONUnmarshallerEngine, error) {
	rType, err := lookupType(typeName)
	if err != nil {
		return nil, nil, err
	}
	return newJSONMarshallerByReflectType(rType, typeName, ioConfig, legacy)
}

func newJSONMarshallerByReflectType(rType reflect.Type, typeName string, ioConfig *config.IOConfig, legacy *legacyjson.Marshaller) (JSONMarshallerEngine, JSONUnmarshallerEngine, error) {
	value := reflect.New(rType).Interface()
	if initializer, ok := value.(JSONRuntimeInitializer); ok {
		if err := initializer.InitJSONRuntime(ioConfig, legacy); err != nil {
			return nil, nil, err
		}
	}
	marshaller, ok := value.(JSONMarshallerEngine)
	if !ok {
		if codec, ok := value.(Marshaller); ok {
			marshaller = marshalCodecAdapter{Marshaller: codec}
		}
	}
	unmarshaller, ok := value.(JSONUnmarshallerEngine)
	if !ok {
		if codec, ok := value.(Unmarshaller); ok {
			unmarshaller = unmarshalCodecAdapter{Unmarshaller: codec}
		}
	}
	if marshaller == nil {
		return nil, nil, fmt.Errorf("invalid type %s: json marshaller engine was not initialized", typeName)
	}
	if unmarshaller == nil {
		return nil, nil, fmt.Errorf("invalid type %s: json unmarshaller engine was not initialized", typeName)
	}
	return marshaller, unmarshaller, nil
}

type marshalCodecAdapter struct {
	Marshaller
}

func (a marshalCodecAdapter) Marshal(src interface{}, _ ...interface{}) ([]byte, error) {
	return a.Marshaller.Marshal(src)
}

type unmarshalCodecAdapter struct {
	Unmarshaller
}

func (a unmarshalCodecAdapter) Unmarshal(bytes []byte, dest interface{}, _ ...interface{}) error {
	return a.Unmarshaller.Unmarshal(bytes, dest)
}

type JSONRuntimeInitializer interface {
	InitJSONRuntime(ioConfig *config.IOConfig, legacy *legacyjson.Marshaller) error
}

type StructologyJSONRuntime struct {
	config *config.IOConfig
}

func (m *StructologyJSONRuntime) InitJSONRuntime(ioConfig *config.IOConfig, _ *legacyjson.Marshaller) error {
	m.config = ioConfig
	return nil
}

func (m *StructologyJSONRuntime) Marshal(src interface{}, options ...interface{}) ([]byte, error) {
	structologyOptions, err := m.marshalOptions(options)
	if err != nil {
		return nil, err
	}
	return structjson.Marshal(src, structologyOptions...)
}

func (m *StructologyJSONRuntime) Unmarshal(bytes []byte, dest interface{}, options ...interface{}) error {
	structologyOptions, err := m.unmarshalOptions(options)
	if err != nil {
		return err
	}
	return structjson.Unmarshal(bytes, dest, structologyOptions...)
}

func (m *StructologyJSONRuntime) marshalOptions(options []interface{}) ([]structjson.Option, error) {
	result := []structjson.Option{
		structjson.WithOmitEmpty(m.config != nil && m.config.OmitEmpty),
		structjson.WithNilSlicePolicy(structjson.NilSliceAsEmptyArray),
	}
	if m.config != nil {
		if caseFormat := m.config.CaseFormat; caseFormat.IsDefined() {
			result = append(result, structjson.WithPathNameTransformer(datlyPathNameTransformer{caseFormat: caseFormat}))
		}
		if timeLayout := m.config.GetTimeLayout(); timeLayout != "" {
			result = append(result, structjson.WithFormatTag(&format.Tag{TimeLayout: timeLayout}))
		}
	}

	var filters []*legacyjson.FilterEntry
	for _, option := range options {
		if option == nil {
			continue
		}
		switch actual := option.(type) {
		case []*legacyjson.FilterEntry:
			filters = append(filters, actual...)
		case legacyjson.MarshalerInterceptors:
			if len(actual) > 0 {
				return nil, fmt.Errorf("structology engine does not support legacy marshal interceptors")
			}
		case *legacyjson.MarshallSession:
			return nil, fmt.Errorf("structology engine does not support legacy marshal sessions")
		default:
			return nil, fmt.Errorf("structology engine does not support marshal option %T", option)
		}
	}

	if excluder := newDatlyPathFieldExcluder(m.config, filters); excluder != nil {
		result = append(result, structjson.WithPathFieldExcluder(excluder))
	}
	return result, nil
}

func (m *StructologyJSONRuntime) unmarshalOptions(options []interface{}) ([]structjson.Option, error) {
	var result []structjson.Option
	if m.config != nil {
		if caseFormat := m.config.CaseFormat; caseFormat.IsDefined() {
			result = append(result, structjson.WithCaseFormat(caseFormat))
		}
		if timeLayout := m.config.GetTimeLayout(); timeLayout != "" {
			result = append(result, structjson.WithFormatTag(&format.Tag{TimeLayout: timeLayout}))
		}
	}

	for _, option := range options {
		if option == nil {
			continue
		}
		switch actual := option.(type) {
		case legacyjson.UnmarshalerInterceptors:
			if len(actual) > 0 {
				return nil, fmt.Errorf("structology engine does not support legacy unmarshal interceptors")
			}
		case *legacyjson.UnmarshalSession:
			return nil, fmt.Errorf("structology engine does not support legacy unmarshal sessions")
		case *http.Request:
			continue
		default:
			return nil, fmt.Errorf("structology engine does not support unmarshal option %T", option)
		}
	}

	return result, nil
}

type LegacyJSONRuntime struct {
	marshaller *legacyjson.Marshaller
}

func (m *LegacyJSONRuntime) InitJSONRuntime(_ *config.IOConfig, legacy *legacyjson.Marshaller) error {
	m.marshaller = legacy
	return nil
}

func (m *LegacyJSONRuntime) Marshal(src interface{}, options ...interface{}) ([]byte, error) {
	if m.marshaller == nil {
		return nil, fmt.Errorf("legacy json runtime was not initialized")
	}
	return m.marshaller.Marshal(src, options...)
}

func (m *LegacyJSONRuntime) Unmarshal(bytes []byte, dest interface{}, options ...interface{}) error {
	if m.marshaller == nil {
		return fmt.Errorf("legacy json runtime was not initialized")
	}
	return m.marshaller.Unmarshal(bytes, dest, options...)
}

type datlyPathFieldExcluder struct {
	exclude map[string]bool
	filters map[string]map[string]bool
}

type datlyPathNameTransformer struct {
	caseFormat text.CaseFormat
}

func newDatlyPathFieldExcluder(ioConfig *config.IOConfig, entries []*legacyjson.FilterEntry) structjson.PathFieldExcluder {
	ret := &datlyPathFieldExcluder{}
	if ioConfig != nil && len(ioConfig.Exclude) > 0 {
		ret.exclude = ioConfig.Exclude
	}
	if len(entries) > 0 {
		ret.filters = make(map[string]map[string]bool, len(entries))
		for _, entry := range entries {
			if entry == nil {
				continue
			}
			fields := make(map[string]bool, len(entry.Fields))
			for _, field := range entry.Fields {
				fields[field] = true
			}
			ret.filters[entry.Path] = fields
			normalizedPath := normalizeFilterPath(entry.Path)
			if normalizedPath != entry.Path {
				ret.filters[normalizedPath] = fields
			}
		}
	}
	if len(ret.exclude) == 0 && len(ret.filters) == 0 {
		return nil
	}
	return ret
}

func (d *datlyPathFieldExcluder) ExcludePath(path []string, fieldName string) bool {
	fullPath := fieldName
	parentPath := ""
	if len(path) > 0 {
		parentPath = strings.Join(path, ".")
		fullPath = parentPath + "." + fieldName
	}
	if len(d.exclude) > 0 {
		if d.exclude[fullPath] || d.exclude[config.NormalizeExclusionKey(fullPath)] {
			return true
		}
	}
	if len(d.filters) == 0 {
		return false
	}
	fields, ok := d.filters[parentPath]
	if !ok {
		fields, ok = d.filters[normalizeFilterPath(parentPath)]
		if !ok {
			return false
		}
	}
	return !fields[fieldName]
}

func normalizeFilterPath(path string) string {
	if path == "" {
		return ""
	}
	return strings.ToLower(strings.ReplaceAll(path, "_", ""))
}

func (d datlyPathNameTransformer) TransformPath(_ []string, fieldName string) string {
	if fieldName == "ID" {
		switch d.caseFormat {
		case text.CaseFormatLower, text.CaseFormatLowerCamel, text.CaseFormatLowerUnderscore:
			return "id"
		case text.CaseFormatUpperCamel, text.CaseFormatUpper, text.CaseFormatUpperUnderscore:
			return "ID"
		}
	}
	fromCaseFormat := text.CaseFormatUpperCamel
	if detected := text.DetectCaseFormat(fieldName); detected.IsDefined() {
		fromCaseFormat = detected
	}
	return fromCaseFormat.Format(fieldName, d.caseFormat)
}
