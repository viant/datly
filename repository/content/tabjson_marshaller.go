package content

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/datly/gateway/router/marshal/tabjson"
	structjsontab "github.com/viant/structology/encoding/jsontab"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
)

var DefaultTabularJSONEngineTypeName = reflect.TypeOf(StructologyTabularJSONRuntime{}).PkgPath() + "/" + reflect.TypeOf(StructologyTabularJSONRuntime{}).Name()

type TabularJSONMarshallerEngine interface {
	Marshal(src interface{}, options ...interface{}) ([]byte, error)
}

type TabularJSONUnmarshallerEngine interface {
	Unmarshal(bytes []byte, dest interface{}) error
}

type TabularJSONRuntimeInitializer interface {
	InitTabularJSONRuntime(cfg *TabularJSONConfig, excludedPaths []string, inputType, outputType reflect.Type) error
}

func newTabularJSONMarshaller(cfg *TabularJSONConfig, inputType, outputType reflect.Type, excludedPaths []string, lookupType xreflect.LookupType) (TabularJSONMarshallerEngine, TabularJSONUnmarshallerEngine, error) {
	typeName := normalizeTabularJSONEngineTypeName(cfg.Engine)
	if rType := xunsafe.LookupType(typeName); rType != nil {
		return newTabularJSONMarshallerByReflectType(rType, typeName, cfg, excludedPaths, inputType, outputType)
	}
	if lookupType == nil {
		return nil, nil, fmt.Errorf("unsupported tabular json marshaller engine: %s", typeName)
	}
	rType, err := lookupType(typeName)
	if err != nil {
		return nil, nil, err
	}
	return newTabularJSONMarshallerByReflectType(rType, typeName, cfg, excludedPaths, inputType, outputType)
}

func normalizeTabularJSONEngineTypeName(engine string) string {
	normalized := strings.TrimSpace(engine)
	if normalized == "" {
		return DefaultTabularJSONEngineTypeName
	}
	return normalized
}

func newTabularJSONMarshallerByReflectType(rType reflect.Type, typeName string, cfg *TabularJSONConfig, excludedPaths []string, inputType, outputType reflect.Type) (TabularJSONMarshallerEngine, TabularJSONUnmarshallerEngine, error) {
	value := reflect.New(rType).Interface()
	if initializer, ok := value.(TabularJSONRuntimeInitializer); ok {
		if err := initializer.InitTabularJSONRuntime(cfg, excludedPaths, inputType, outputType); err != nil {
			return nil, nil, err
		}
	}
	marshaller, ok := value.(TabularJSONMarshallerEngine)
	if !ok {
		return nil, nil, fmt.Errorf("invalid type %s: tabular json marshaller engine was not initialized", typeName)
	}
	unmarshaller, ok := value.(TabularJSONUnmarshallerEngine)
	if !ok {
		return nil, nil, fmt.Errorf("invalid type %s: tabular json unmarshaller engine was not initialized", typeName)
	}
	return marshaller, unmarshaller, nil
}

type StructologyTabularJSONRuntime struct {
	config *TabularJSONConfig
}

func (m *StructologyTabularJSONRuntime) InitTabularJSONRuntime(cfg *TabularJSONConfig, _ []string, _ reflect.Type, _ reflect.Type) error {
	m.config = cfg
	return nil
}

func (m *StructologyTabularJSONRuntime) Marshal(src interface{}, options ...interface{}) ([]byte, error) {
	jsontabOptions, err := m.marshalOptions(options)
	if err != nil {
		return nil, err
	}
	return structjsontab.Marshal(src, jsontabOptions...)
}

func (m *StructologyTabularJSONRuntime) Unmarshal(bytes []byte, dest interface{}) error {
	return structjsontab.Unmarshal(bytes, dest, m.unmarshalOptions()...)
}

func (m *StructologyTabularJSONRuntime) marshalOptions(options []interface{}) ([]structjsontab.Option, error) {
	result := []structjsontab.Option{
		structjsontab.WithTagName(tabjson.TagName),
	}
	if m.config != nil && m.config.FloatPrecision != "" {
		precision, err := strconv.Atoi(strings.TrimSpace(m.config.FloatPrecision))
		if err != nil {
			return nil, fmt.Errorf("invalid tabular json float precision %q: %w", m.config.FloatPrecision, err)
		}
		result = append(result, structjsontab.WithFloatPrecision(precision))
	}
	for _, option := range options {
		if option == nil {
			continue
		}
		switch actual := option.(type) {
		case []*tabjson.Config:
			if len(actual) > 0 {
				return nil, fmt.Errorf("structology tabular json engine does not support legacy depth configs")
			}
		default:
			return nil, fmt.Errorf("structology tabular json engine does not support marshal option %T", option)
		}
	}
	return result, nil
}

func (m *StructologyTabularJSONRuntime) unmarshalOptions() []structjsontab.Option {
	return []structjsontab.Option{
		structjsontab.WithTagName(tabjson.TagName),
	}
}

type LegacyTabularJSONRuntime struct {
	input  *tabjson.Marshaller
	output *tabjson.Marshaller
}

func (m *LegacyTabularJSONRuntime) InitTabularJSONRuntime(cfg *TabularJSONConfig, excludedPaths []string, inputType, outputType reflect.Type) error {
	configured := ensureTabularJSONConfig(cfg, excludedPaths)
	var err error
	m.output, err = newLegacyTabularOutputMarshaller(outputType, configured._config)
	if err != nil {
		return err
	}
	if inputType == nil {
		return nil
	}
	m.input, err = tabjson.NewMarshaller(inputType, nil)
	return err
}

func (m *LegacyTabularJSONRuntime) Marshal(src interface{}, options ...interface{}) ([]byte, error) {
	if m.output == nil {
		return nil, fmt.Errorf("legacy tabular json runtime was not initialized")
	}
	return m.output.Marshal(src, options...)
}

func (m *LegacyTabularJSONRuntime) Unmarshal(bytes []byte, dest interface{}) error {
	if m.input == nil {
		return fmt.Errorf("legacy tabular json runtime was not initialized")
	}
	return m.input.Unmarshal(bytes, dest)
}

func ensureTabularJSONConfig(cfg *TabularJSONConfig, excludedPaths []string) *TabularJSONConfig {
	if cfg == nil {
		cfg = &TabularJSONConfig{}
	}
	if cfg._config == nil {
		cfg._config = &tabjson.Config{}
	}
	if cfg._config.FieldSeparator == "" {
		cfg._config.FieldSeparator = ","
	}
	if cfg._config.NullValue == "" {
		cfg._config.NullValue = "null"
	}
	if cfg.FloatPrecision != "" {
		cfg._config.StringifierConfig.StringifierFloat32Config.Precision = cfg.FloatPrecision
		cfg._config.StringifierConfig.StringifierFloat64Config.Precision = cfg.FloatPrecision
	}
	cfg._config.ExcludedPaths = excludedPaths
	return cfg
}

func newLegacyTabularOutputMarshaller(outputType reflect.Type, cfg *tabjson.Config) (*tabjson.Marshaller, error) {
	if outputType == nil {
		return nil, nil
	}
	if outputType.Kind() == reflect.Ptr {
		outputType = outputType.Elem()
	}
	return tabjson.NewMarshaller(outputType, cfg)
}
