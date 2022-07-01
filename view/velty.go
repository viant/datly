package view

import (
	"context"
	"fmt"
	"github.com/viant/datly/converter"
	"github.com/viant/datly/view/keywords"
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

	Criteria = "criteria"
)

type CriteriaSanitizer struct {
	Columns      ColumnIndex
	Placeholders []interface{}
}

func (c *CriteriaSanitizer) AsBinding(value interface{}) string {
	c.Placeholders = append(c.Placeholders, value)
	return "?"
}

func (c *CriteriaSanitizer) AsColumn(columnName string) (string, error) {
	lookup, err := c.Columns.Lookup(columnName)
	if err != nil {
		return "", err
	}

	return lookup.Name, nil
}

type Sanitizer struct {
	sanitize   func(id, criteria string, value interface{}, placeholders *[]interface{}, columns ColumnIndex) (string, error)
	keyword    string
	isFunction bool
}

var sanitizers = []*Sanitizer{
	{
		sanitize: func(id, criteria string, value interface{}, placeholders *[]interface{}, columns ColumnIndex) (string, error) {
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
		sanitize: func(id, criteria string, value interface{}, placeholders *[]interface{}, columns ColumnIndex) (string, error) {
			*placeholders = append(*placeholders, value)
			return strings.Replace(criteria, id, "?", 1), nil
		},
		keyword: SafeValue,
	},
	{
		sanitize: func(id, criteria string, value interface{}, placeholders *[]interface{}, columns ColumnIndex) (string, error) {
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
		keyword:    SafeInt,
		isFunction: true,
	},
	{
		sanitize: func(id, criteria string, value interface{}, placeholders *[]interface{}, columns ColumnIndex) (string, error) {
			raw, ok := value.(string)
			if !ok {
				return "", fmt.Errorf("expected value to be type of string but was %T", value)
			}

			*placeholders = append(*placeholders, raw)
			return strings.Replace(criteria, id, "?", 1), nil
		},
		keyword:    SafeString,
		isFunction: true,
	},
	{
		sanitize: func(id, criteria string, value interface{}, placeholders *[]interface{}, columns ColumnIndex) (string, error) {
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
		keyword:    SafeBool,
		isFunction: true,
	},
	{
		sanitize: func(id, criteria string, value interface{}, placeholders *[]interface{}, columns ColumnIndex) (string, error) {
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
		keyword:    SafeFloat,
		isFunction: true,
	},
}

type VeltyCodec struct {
	template  string
	codecType reflect.Type
	newState  func() *est.State
	executor  *est.Execution
	columns   ColumnIndex
	accessors *Accessors
}

func (v *VeltyCodec) Value(ctx context.Context, raw string, options ...interface{}) (interface{}, error) {
	raw = strings.TrimSpace(raw)
	selector := v.selector(options)
	if selector == nil {
		return nil, fmt.Errorf("expected selector not to be nil")
	}

	aValue, wasNil, err := converter.Convert(raw, v.codecType, "")
	if err != nil {
		return nil, err
	}

	criteria, err := v.evaluateCriteria(selector, aValue, wasNil)
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
			var selectorSanitizer *Sanitizer
			for _, sanitizer := range sanitizers {
				if strings.HasPrefix(selectName, sanitizer.keyword) {
					if sanitizer.isFunction {
						selectName = selectName[len(sanitizer.keyword)+1 : len(selectName)-1]
					} else {
						selectName = selectName[len(sanitizer.keyword)+1 : len(selectName)]
					}
					selectorSanitizer = sanitizer
					break
				}
			}

			if selectorSanitizer == nil {
				continue
			}

			if selectorSanitizer.isFunction {
				criteria, err = selectorSanitizer.sanitize(actual.FullName, criteria, selectName, &selector.Placeholders, v.columns)
				if err != nil {
					return nil, err
				}
				continue
			}

			value, err := v.extractValue(selectName, aValue)
			if err != nil {
				return nil, err
			}

			criteria, err = selectorSanitizer.sanitize(actual.FullName, criteria, value, &selector.Placeholders, v.columns)
			if err != nil {
				return nil, err
			}
		}
	}

	selector.Criteria = criteria

	return nil, nil
}

func (v *VeltyCodec) evaluateCriteria(selector *Selector, dest interface{}, wasNil bool) (string, error) {
	state := v.newState()
	if !wasNil {
		if err := state.SetValue(keywords.ParamsKey, dest); err != nil {
			return "", err
		}
	}

	criteriaSanitizer := NewCriteria(v.columns)
	if err := state.SetValue(Criteria, criteriaSanitizer); err != nil {
		return "", err
	}

	if err := v.executor.Exec(state); err != nil {
		return "", err
	}

	selector.Placeholders = append(selector.Placeholders, criteriaSanitizer.Placeholders...)

	return state.Buffer.String(), nil
}

func NewCriteria(columns ColumnIndex) *CriteriaSanitizer {
	return &CriteriaSanitizer{
		Columns:      columns,
		Placeholders: []interface{}{},
	}
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

func (v *VeltyCodec) extractValue(selectName string, dest interface{}) (interface{}, error) {
	path, indexes, err := extractIndexes(selectName)
	if err != nil {
		return nil, err
	}

	accessor, err := v.accessors.AccessorByName(path)
	if err != nil {
		return nil, err
	}

	value, err := accessor.Value(dest, indexes...)
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

	var columns ColumnIndex
	if view != nil {
		columns = view._columns
	}

	codec := &VeltyCodec{
		template:  template,
		codecType: paramType,
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
	v.accessors.init(v.codecType)

	planner := velty.New()

	var err error
	if err = planner.DefineVariable(keywords.ParamsKey, v.codecType); err != nil {
		return err
	}

	if err = planner.DefineVariable(SafeColumn, ""); err != nil {
		return err
	}

	if err = planner.DefineVariable(Criteria, &CriteriaSanitizer{}); err != nil {
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
