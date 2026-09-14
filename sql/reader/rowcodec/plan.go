// Package rowcodec compiles column codecs into typed SQLX scan wrappers.
package rowcodec

import (
	"fmt"
	"io/fs"
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/datly/data"
	xshape "github.com/viant/x/shape"
	xcodec "github.com/viant/xdatly/codec"
)

type Config struct {
	RowType    reflect.Type
	Columns    []*data.Column
	Factory    xcodec.Factory
	LookupType func(string) (reflect.Type, error)
	Resources  fs.FS
}

// Plan contains immutable source/destination contracts; Decode state is per read.
type Plan struct {
	modelType reflect.Type
	scanType  reflect.Type
	bindings  []binding
	copies    []fieldCopy
}

type binding struct {
	name       string
	rawName    string
	sourceType reflect.Type
	target     *xshape.Accessor
	instance   xcodec.Instance
}
type fieldCopy struct {
	index  int
	name   string
	target *xshape.Accessor
}

func Compile(config Config) (*Plan, error) {
	var columns []*data.Column
	for _, column := range config.Columns {
		if column != nil && column.Codec != nil {
			columns = append(columns, column)
		}
	}
	if len(columns) == 0 {
		return nil, nil
	}
	if config.Factory == nil {
		return nil, fmt.Errorf("column codec factory is required")
	}
	owner := xshape.Linked(config.RowType)
	model := (xshape.Runtime{}).Indirect(config.RowType)
	if model == nil || model.Kind() != reflect.Struct {
		return nil, fmt.Errorf("column codecs require a typed struct row")
	}
	plan := &Plan{modelType: model}
	runtime := xshape.Runtime{Lookup: config.LookupType}
	fields, err := owner.Fields()
	if err != nil {
		return nil, err
	}
	fieldNames := map[string]bool{}
	for _, field := range fields {
		fieldNames[field.Name] = true
	}
	var raw []xshape.RuntimeField
	codecNames := map[string]bool{}
	for i, column := range columns {
		target, err := owner.Accessor(column.Name)
		if err != nil {
			return nil, fmt.Errorf("codec column %s: %w", column.Name, err)
		}
		if strings.TrimSpace(column.DataType) == "" {
			return nil, fmt.Errorf("codec column %s requires an explicit source data type", column.Name)
		}
		source, err := runtime.Type(column.DataType)
		if err != nil {
			return nil, fmt.Errorf("codec column %s source type: %w", column.Name, err)
		}
		if column.Codec.OutputType != "" {
			declared, err := runtime.Type(column.Codec.OutputType)
			if err != nil {
				return nil, err
			}
			if declared != target.Type() {
				return nil, fmt.Errorf("codec column %s output type %s does not match %s", column.Name, declared, target.Type())
			}
		}
		instance, err := config.Factory.New(&xcodec.Config{Body: column.Codec.Body, SourceType: source, DestinationType: target.Type(), Args: append([]string(nil), column.Codec.Args...), OutputTypeExpression: column.Codec.OutputType}, xcodec.WithTypeLookup(config.LookupType), xcodec.WithResourceFS(config.Resources))
		if err != nil {
			return nil, fmt.Errorf("compile column codec %s: %w", column.Name, err)
		}
		if instance == nil || reflect.ValueOf(instance).Kind() == reflect.Pointer && reflect.ValueOf(instance).IsNil() {
			return nil, fmt.Errorf("column codec %s returned a nil instance", column.Name)
		}
		rawName := fmt.Sprintf("Codec%d", i)
		for fieldNames[rawName] {
			rawName = "Raw" + rawName
		}
		aliases := column.Column
		if aliases == "" {
			aliases = column.Name
		} else {
			aliases += "|" + column.Name
		}
		scanType := source
		if scanType.Kind() != reflect.Pointer {
			scanType = runtime.Pointer(scanType)
		}
		raw = append(raw, xshape.RuntimeField{Name: rawName, Type: scanType, Tag: reflect.StructTag("sqlx:" + strconv.Quote(aliases))})
		plan.bindings = append(plan.bindings, binding{name: column.Name, rawName: rawName, sourceType: source, target: target, instance: instance})
		codecNames[column.Name] = true
	}
	var shadow []xshape.RuntimeField
	for _, field := range fields {
		if !field.Exported {
			continue
		}
		if field.Anonymous && runtime.Indirect(field.ReflectedType).Kind() == reflect.Struct && !codecNames[field.Name] {
			continue
		}
		accessor, err := owner.Accessor(field.Name)
		if err != nil {
			return nil, err
		}
		plan.copies = append(plan.copies, fieldCopy{index: len(shadow), name: field.Name, target: accessor})
		shadow = append(shadow, xshape.RuntimeField{Name: field.Name, Type: field.ReflectedType, Tag: field.Tag})
	}
	rawType, err := runtime.Struct(raw)
	if err != nil {
		return nil, err
	}
	shadowType, err := runtime.Struct(shadow)
	if err != nil {
		return nil, err
	}
	plan.scanType, err = runtime.Struct([]xshape.RuntimeField{{Name: "Raw", Type: rawType, Anonymous: true}, {Name: "Shadow", Type: shadowType, Anonymous: true}, {Name: "Actual", Type: runtime.Pointer(model), Tag: `sqlx:"-"`}})
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func (p *Plan) ModelType() reflect.Type {
	if p == nil {
		return nil
	}
	return p.modelType
}
