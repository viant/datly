package view

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/velty"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/est"
	"github.com/viant/velty/parser"
	"reflect"
	"strconv"
	"strings"
)

const (
	SafeColumn = "Safe_Column"
	SafeValue  = "Safe_Value"
	SafeInt    = "Safe_Int"
	SafeString = "Safe_String"
	SafeBool   = "Safe_Bool"
	SafeFloat  = "Safe_Float"
)

type Sanitizer struct {
	sanitize func(id, criteria string, value interface{}, placeholders *[]interface{}, columns *Columns) (string, error)
	keyword  string
}

var sanitizers = []*Sanitizer{
	{
		sanitize: func(id, criteria string, value interface{}, placeholders *[]interface{}, columns *Columns) (string, error) {
			columnName, ok := value.(string)
			if !ok {
				return "", fmt.Errorf("expected column name to be type of string but was %T", value)
			}

			column, err := columns.Lookup(columnName)
			if err != nil {
				return "", err
			}
			return strings.Replace(criteria, id, column.Name, 1), nil
		},
		keyword: SafeColumn,
	},
	{
		sanitize: func(id, criteria string, value interface{}, placeholders *[]interface{}, columns *Columns) (string, error) {
			*placeholders = append(*placeholders, value)
			return strings.Replace(criteria, id, "?", 1), nil
		},
		keyword: SafeValue,
	},
	{
		sanitize: func(id, criteria string, value interface{}, placeholders *[]interface{}, columns *Columns) (string, error) {
			raw, ok := value.(string)
			if !ok {
				return "", fmt.Errorf("expected value to be type of string but was %T", value)
			}

			asInt, err := strconv.Atoi(raw)

			if err != nil {
				return "", err
			}

			*placeholders = append(*placeholders, asInt)
			return strings.Replace(criteria, id, "?", 1), nil
		},
		keyword: SafeInt,
	},
	{
		sanitize: func(id, criteria string, value interface{}, placeholders *[]interface{}, columns *Columns) (string, error) {
			raw, ok := value.(string)
			if !ok {
				return "", fmt.Errorf("expected value to be type of string but was %T", value)
			}

			*placeholders = append(*placeholders, raw)
			return strings.Replace(criteria, id, "?", 1), nil
		},
		keyword: SafeString,
	},
	{
		sanitize: func(id, criteria string, value interface{}, placeholders *[]interface{}, columns *Columns) (string, error) {
			raw, ok := value.(string)
			if !ok {
				return "", fmt.Errorf("expected value to be type of string but was %T", value)
			}

			asBool, err := strconv.ParseBool(raw)
			if err != nil {
				return "", err
			}

			*placeholders = append(*placeholders, asBool)
			return strings.Replace(criteria, id, "?", 1), nil
		},
		keyword: SafeBool,
	},
	{
		sanitize: func(id, criteria string, value interface{}, placeholders *[]interface{}, columns *Columns) (string, error) {
			raw, ok := value.(string)
			if !ok {
				return "", fmt.Errorf("expected value to be type of string but was %T", value)
			}

			asFloat, err := strconv.ParseFloat(raw, 64)
			if err != nil {
				return "", err
			}

			*placeholders = append(*placeholders, asFloat)
			return strings.Replace(criteria, id, "?", 1), nil
		},
		keyword: SafeFloat,
	},
}

type VeltyCodec struct {
	template  string
	paramType reflect.Type
	newState  func() *est.State
	executor  *est.Execution
	columns   Columns
	accessors *Accessors
}

func (v *VeltyCodec) Value(ctx context.Context, raw string, options ...interface{}) (interface{}, error) {
	selector := v.selector(options)
	if selector == nil {
		return nil, fmt.Errorf("expected selector not to be nil")
	}

	dest := reflect.New(v.paramType)
	if err := json.Unmarshal([]byte(raw), dest.Interface()); err != nil {
		return nil, err
	}

	criteria, err := v.evaluateCriteria(dest)
	if err != nil {
		return nil, err
	}

	criteria = strings.TrimSpace(criteria)
	block, err := parser.Parse([]byte(criteria))
	if err != nil {
		return nil, err
	}

	for _, statement := range block.Statements() {
		switch actual := statement.(type) {
		case *expr.Select:
			actualId := extractActualId(actual.FullName)

			selectName := actualId
			dotIndex := strings.Index(selectName, ".")
			var prefix string
			if dotIndex != -1 {
				prefix = selectName[:dotIndex]
			}

			value, err := v.extractValue(selectName, dest)
			if err != nil {
				return nil, err
			}

			switch prefix {
			case SafeColumn:
				columnName := value.(string)
				_, err = v.columns.Lookup(columnName)
				if err != nil {
					return nil, err
				}
				criteria = strings.Replace(criteria, actual.FullName, columnName, 1)
			default:
				selector.Placeholders = append(selector.Placeholders, value)
				criteria = strings.Replace(criteria, actual.FullName, "?", 1)
			}
		}
	}

	selector.Criteria = criteria

	return nil, nil
}

func (v *VeltyCodec) evaluateCriteria(dest reflect.Value) (string, error) {
	state := v.newState()
	if !dest.IsNil() {
		if err := state.SetValue("Unsafe", dest.Elem().Interface()); err != nil {
			return "", err
		}
	}

	if err := state.SetValue(SafeColumn, "$COLUMN_NAME"); err != nil {
		return "", err
	}

	if err := state.SetValue(SafeValue, "$COLUMN_VALUE"); err != nil {
		return "", err
	}

	v.executor.Exec(state)
	return state.Buffer.String(), nil
}

func extractActualId(name string) string {
	if len(name) == 0 {
		return name
	}

	name = name[1:] // skip $
	if len(name) > 0 && byte(name[0]) == '{' && byte(name[len(name)-1]) == '}' {
		name = name[1 : len(name)-1]
	}

	return name
}

func (v *VeltyCodec) selector(options []interface{}) *Selector {
	var selector *Selector
	for _, option := range options {
		switch actual := option.(type) {
		case *Selector:
			selector = actual
		}
	}

	return selector
}

func (v *VeltyCodec) extractValue(selectName string, dest reflect.Value) (interface{}, error) {
	path, indexes, err := extractIndexes(selectName)
	if err != nil {
		return nil, err
	}

	accessor, err := v.accessors.AccessorByName(path)
	if err != nil {
		return nil, err
	}

	value, err := accessor.Value(dest.Elem().Interface(), indexes...)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func extractIndexes(name string) (string, []int, error) {
	indexes := make([]int, 0)
	lastIndex := 0
	for {
		lastIndex = strings.IndexByte(name[lastIndex:], '[')
		if lastIndex == -1 {
			return name, indexes, nil
		}

		indexEnclose := strings.IndexByte(name[lastIndex:], ']')
		if indexEnclose == -1 {
			return "", nil, fmt.Errorf("expected to find ']' in %v", name[lastIndex:])
		}

		index, err := strconv.Atoi(name[lastIndex+1 : indexEnclose+lastIndex])
		if err != nil {
			return "", nil, err
		}
		indexes = append(indexes, index)
		name = name[:lastIndex] + name[indexEnclose+lastIndex+1:]
		lastIndex = 0
	}
}

func NewVeltyCodec(template string, paramType reflect.Type, view *View) (*VeltyCodec, error) {
	template = escapeSafeKeywords(template)

	var columns Columns
	if view != nil {
		columns = view._columns
	}

	codec := &VeltyCodec{
		template:  template,
		paramType: paramType,
		columns:   columns,
	}

	if err := codec.init(); err != nil {
		return nil, err
	}

	return codec, nil
}

func escapeSafeKeywords(template string) string {
	for _, keyword := range sanitizers {
		template = strings.ReplaceAll(template, "${"+keyword.keyword, "#[[$]]#{"+keyword.keyword)
		template = strings.ReplaceAll(template, "$"+keyword.keyword, "#[[$]]#"+keyword.keyword)
	}

	return template
}

func (v *VeltyCodec) init() error {
	v.accessors = &Accessors{index: map[string]int{}}
	v.accessors.init(v.paramType)

	planner := velty.New()

	var err error
	if err = planner.DefineVariable("Unsafe", v.paramType); err != nil {
		return err
	}

	if err = planner.DefineVariable(SafeColumn, ""); err != nil {
		return err
	}

	if err = planner.DefineVariable(SafeValue, ""); err != nil {
		return err
	}

	v.executor, v.newState, err = planner.Compile([]byte(v.template))
	if err != nil {
		return err
	}

	return nil
}
