package config

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
	"sync"
)

type (
	JSONFactory struct{}

	JSONParsers struct {
		aMap          sync.Map // key: reflectType, value *JSONParser
		lookup        xreflect.LookupType
		aType         string
		resultType    reflect.Type
		isGenericType bool
		genericPath   string
	}

	JSONParser struct {
		mux         sync.Mutex
		types       map[string]reflect.Type
		typesLookup xreflect.LookupType
		accessor    *types.Accessor
	}

	ParentValue struct {
		Value interface{}
		RType reflect.Type
	}
)

func (j *JSONParsers) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return j.resultType, nil
}

func (p *JSONParser) Value(parent interface{}) (string, error) {
	value, err := p.accessor.Value(xunsafe.AsPointer(parent))
	if err != nil {
		return "", err
	}

	aString, ok := asString(value)
	if !ok {
		return "", UnexpectedValueType(aString, value)
	}

	return aString, nil
}

func (j *JSONParsers) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
	aString, ok := asString(raw)
	if !ok {
		return nil, UnexpectedValueType(aString, raw)
	}

	opts := NewOptions(codec.NewOptions(options))
	parent := opts.ParentValue
	rType, err := j.getActualType(parent)
	if err != nil {
		return nil, err
	}

	if strings.HasPrefix(strings.TrimSpace(aString), "[") {
		rType = reflect.SliceOf(rType)
	}
	result := reflect.New(rType)
	if err := j.unmarshalIfNotEmpty(aString, result); err != nil {
		return nil, err
	}

	return result.Elem().Interface(), nil
}

func (j *JSONParsers) unmarshalIfNotEmpty(aString string, result reflect.Value) error {
	if aString == "" {
		return nil
	}

	if err := json.Unmarshal([]byte(aString), result.Interface()); err != nil {
		return err
	}

	return nil
}

func (j *JSONFactory) New(codecConfig *codec.Config, options ...codec.Option) (codec.Instance, error) {
	typeName := codecConfig.OutputType
	if typeName == "" {
		if err := ValidateArgs(codecConfig, 1, CodecJSON); err != nil {
			return nil, err
		}

		typeName = codecConfig.Args[0]
	}
	opts := NewOptions(codec.NewOptions(options))
	lookup := opts.LookupType
	recordPrefix := keywords.Rec + "."
	isGenericType := strings.HasPrefix(typeName, recordPrefix)
	var genericPath string

	var resultType reflect.Type
	if isGenericType {
		resultType = xreflect.InterfaceType
		genericPath = strings.Replace(typeName, recordPrefix, "", 1)
	} else {
		parsed, err := types.LookupType(lookup, typeName)
		if err != nil {
			return nil, err
		}
		resultType = parsed
	}
	result := &JSONParsers{
		aType:         typeName,
		lookup:        lookup,
		aMap:          sync.Map{},
		resultType:    resultType,
		isGenericType: isGenericType,
		genericPath:   genericPath,
	}
	return result, nil
}

func (j *JSONParsers) getActualType(parentValue *ParentValue) (reflect.Type, error) {
	if !j.isGenericType {
		return j.resultType, nil
	}

	if parentValue.RType == nil {
		parentValue.RType = reflect.TypeOf(parentValue.Value)
	}

	parser, err := j.getOrLoadParser(parentValue.RType)
	if err != nil {
		return nil, err
	}

	typeName, err := parser.Value(parentValue.Value)
	if err != nil {
		return nil, err
	}

	return parser.ParseType(typeName)
}

func (j *JSONParsers) getOrLoadParser(rType reflect.Type) (*JSONParser, error) {
	value, ok := j.aMap.Load(rType)
	if ok {
		parser, ok := value.(*JSONParser)
		if ok {
			return parser, nil
		}
	}

	accessors := types.NewAccessors(&types.SqlxNamer{})
	accessors.InitPath(rType, j.genericPath)

	accessor, err := accessors.AccessorByName(j.genericPath)
	if err != nil {
		return nil, err
	}

	result := &JSONParser{
		types:       map[string]reflect.Type{},
		typesLookup: j.lookup,
		accessor:    accessor,
	}

	j.aMap.Store(rType, result)
	return result, err
}

func (p *JSONParser) ParseType(typeName string) (reflect.Type, error) {
	rType, ok := p.types[typeName]
	if ok {
		return rType, nil
	}

	rType, err := types.LookupType(p.typesLookup, typeName)
	if err != nil {
		return nil, err
	}

	p.mux.Lock()
	defer p.mux.Unlock()
	p.types[typeName] = rType
	return rType, nil
}

func asString(raw interface{}) (string, bool) {
	rawString, ok := raw.(string)
	if ok {
		return rawString, true
	}

	strPtr, ok := raw.(*string)
	if ok {
		if strPtr != nil {
			return *strPtr, true
		}
		return "", true
	}

	return "", false
}
