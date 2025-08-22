package expand

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/viant/datly/service/executor/sequencer"
	"github.com/viant/sqlx/io/validator"
	"github.com/viant/toolbox"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xunsafe"
)

type (
	DataUnit struct {
		Columns     codec.ColumnsSource
		ParamsGroup []interface{}
		Mock        bool
		TemplateSQL string
		MetaSource  Dber        `velty:"-"`
		Statements  *Statements `velty:"-"`

		mu                 sync.Mutex                      `velty:"-"`
		placeholderCounter int                             `velty:"-"`
		sqlxValidator      *validator.Service              `velty:"-"`
		sliceIndex         map[reflect.Type]*xunsafe.Slice `velty:"-"`
		ctx                context.Context                 `velty:"-"`
	}

	ExecutablesIndex map[string]*Executable
)

func (c *DataUnit) WithPresence() interface{} {
	var opt interface{} = validator.WithSetMarker()
	return opt
}
func (c *DataUnit) WithLocation(loc string) interface{} {
	var opt interface{} = validator.WithLocation(loc)
	return opt
}

// Reset clears binding-related state so DataUnit can be safely reused for a new evaluation
func (c *DataUnit) Reset() {
	c.mu.Lock()
	c.placeholderCounter = 0
	if len(c.ParamsGroup) > 0 {
		c.ParamsGroup = c.ParamsGroup[:0]
	}
	c.TemplateSQL = ""
	c.mu.Unlock()
}

func (c *DataUnit) Validate(dest interface{}, opts ...interface{}) (*validator.Validation, error) {
	db, err := c.MetaSource.Db()
	if err != nil {
		fmt.Printf("error occured while connecting to DB %v\n", err.Error())
		return nil, fmt.Errorf("error occurred while connecting to DB")
	}
	if c.sqlxValidator == nil {
		c.sqlxValidator = validator.New()
	}
	var options []validator.Option
	for _, opt := range opts {
		if o, ok := (opt).(validator.Option); ok {
			options = append(options, o)
		}
	}
	return c.sqlxValidator.Validate(context.Background(), db, dest, options...)
}

func (c *DataUnit) Allocate(tableName string, dest interface{}, selector string) (string, error) {
	db, err := c.MetaSource.Db()
	if err != nil {
		fmt.Printf("error occured while connecting to DB %v\n", err.Error())
		return "", fmt.Errorf("error occurred while connecting to DB")
	}

	service := sequencer.New(context.Background(), db)
	return "", service.Next(tableName, dest, selector)
}

func (c *DataUnit) AsBinding(value interface{}) (string, error) {
	return c.Add(0, value)
}

func (c *DataUnit) AppendBinding(value interface{}) (string, error) {
	return c.Add(0, value)
}

func (c *DataUnit) UUID() string {
	newUUID := uuid.New()
	c.mu.Lock()
	c.ParamsGroup = append(c.ParamsGroup, newUUID.String())
	c.mu.Unlock()
	return "?"
}

func (c *DataUnit) AsColumn(columnName string) (string, error) {
	return c.Columns.ColumnName(columnName)
}

func (c *DataUnit) Add(_ int, value interface{}) (string, error) {
	if value == nil {
		return "", nil
	}

	lookup, err := bindingsCache.Lookup(value)
	if err != nil {
		return "", err
	}

	expanded, err := lookup.Expand("", value)
	if err != nil {
		return "", err
	}

	result := c.expandWithCommas(expanded)
	return result, nil
}

func (c *DataUnit) expandWithCommas(expanded []*Expression) string {
	sb := &strings.Builder{}
	for i, expression := range expanded {
		if i != 0 {
			sb.WriteString(", ")
		}

		sb.WriteString(expression.SQLFragment.String())
		c.addAll(expression.Args...)

	}

	result := sb.String()
	return result
}

func (c *DataUnit) At(_ int) []interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ParamsGroup
}

func (c *DataUnit) Next() (interface{}, error) {
	if c.Mock {
		return 0, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.placeholderCounter < len(c.ParamsGroup) {
		index := c.placeholderCounter
		c.placeholderCounter++
		return c.ParamsGroup[index], nil
	}

	return nil, fmt.Errorf("expected to get binding parameter, but noone was found, ParamsGroup: %v, placeholderCounter: %v", c.ParamsGroup, c.placeholderCounter)
}

func (c *DataUnit) ensureSliceIndex() {
	if c.sliceIndex != nil {
		return
	}

	c.sliceIndex = map[reflect.Type]*xunsafe.Slice{}
}

func (c *DataUnit) xunsafeSlice(valueType reflect.Type) *xunsafe.Slice {
	slice, ok := c.sliceIndex[valueType]
	if !ok {
		slice = xunsafe.NewSlice(reflect.SliceOf(valueType))
		c.sliceIndex[valueType] = slice
	}

	return slice
}

func (c *DataUnit) addAll(args ...interface{}) {
	if len(args) == 0 {
		return
	}
	c.mu.Lock()
	c.ParamsGroup = append(c.ParamsGroup, args...)
	c.mu.Unlock()
}

func (c *DataUnit) IsServiceExec(SQL string) (*Executable, bool) {
	return c.Statements.LookupExecutable(SQL)
}

func (c *DataUnit) FilterExecutables(statements []string, stopOnNonExec bool) []*Executable {
	result := make([]*Executable, 0)

	for i := 0; i < len(statements); i++ {
		if len(c.Statements.Executable) <= i {
			break
		}

		executable, ok := c.Statements.LookupExecutable(statements[i])
		if !ok && stopOnNonExec {
			return result
		}

		result = append(result, executable)
	}

	return result
}

func (c *DataUnit) In(columnName string, args interface{}) (string, error) {
	return c.in(columnName, args, true)
}

func (c *DataUnit) in(columnName string, args interface{}, inclusive bool) (string, error) {
	expander, err := bindingsCache.Lookup(args)
	if err != nil {
		return "", err
	}

	if !expander.HasAny(args) {
		if !inclusive {
			return "0 = 0", err
		}

		return "1 = 0", nil
	}

	expression, err := expander.Expand(columnName, args)
	if err != nil {
		return "", err
	}

	result := c.expandInExpression(expression, inclusive)
	return result, nil
}

func (c *DataUnit) expandInExpression(expressions []*Expression, inclusive bool) string {
	sb := &strings.Builder{}
	sb.WriteString(" (")

	for i, expression := range expressions {
		if i > 0 {
			if inclusive {
				sb.WriteString(" OR ")
			} else {
				sb.WriteString(" AND ")
			}
		}

		sb.WriteString(expression.ColumnExpression)
		if !inclusive {
			sb.WriteString(" NOT")
		}

		sb.WriteString(" IN ( ")
		sb.WriteString(expression.SQLFragment.String())
		sb.WriteString(")")
		c.addAll(expression.Args...)
	}

	sb.WriteString(" )")

	result := sb.String()
	return result
}

func (c *DataUnit) NotIn(columnName string, args interface{}) (string, error) {
	return c.in(columnName, args, false)
}

func (c *DataUnit) Like(columnName string, args interface{}) (string, error) {
	return c.like(columnName, args, true)
}

func (c *DataUnit) NotLike(columnName string, args interface{}) (string, error) {
	return c.like(columnName, args, false)
}

func (c *DataUnit) like(columnName string, args interface{}, inclusive bool) (string, error) {
	expander, err := bindingsCache.Lookup(args)
	if err != nil {
		return "", err
	}

	if !expander.HasAny(args) {
		if !inclusive {
			return "0 = 0", err
		}

		return "1 = 0", nil
	}

	expressions, err := expander.Expand(columnName, args)
	if err != nil {
		return "", err
	}

	conjunction := " OR "
	if !inclusive {
		conjunction = " AND "
	}

	return c.expandLikeColumnExpressions(expressions, conjunction, inclusive, true), nil
}

func (c *DataUnit) Contains(columnName string, args interface{}) (string, error) {
	return c.contains(columnName, args, true)
}

func (c *DataUnit) NotContains(columnName string, args interface{}) (string, error) {
	return c.contains(columnName, args, false)
}

func (c *DataUnit) contains(columnName string, args interface{}, inclusive bool) (string, error) {
	expander, err := bindingsCache.Lookup(args)
	if err != nil {
		return "", err
	}
	if !expander.HasAny(args) {
		if !inclusive {
			return "0 = 0", err
		}

		return "1 = 0", nil
	}

	expressions, err := expander.Expand(columnName, args)
	if err != nil {
		return "", err
	}

	conjunction := " OR "
	if !inclusive {
		conjunction = " AND "
	}

	return c.expandLikeColumnExpressions(expressions, conjunction, inclusive, false), nil
}

func (c *DataUnit) Delete(data interface{}, name string) (string, error) {
	return c.Statements.DeleteWithMarker(name, data), nil
}

func (c *DataUnit) expandLikeColumnExpressions(expressions []*Expression, conjunction string, inclusive bool, anyMatch bool) string {
	sb := &strings.Builder{}
	shouldPutBrackets := len(expressions) > 1
	for i, expression := range expressions {
		if i != 0 {
			sb.WriteString(" ")
			sb.WriteString(conjunction)
			sb.WriteString(" ")
		}

		if shouldPutBrackets || len(expression.Args) > 1 {
			sb.WriteString("(")
		}

		var likeValues []interface{}
		for i, value := range expression.Args {
			if i > 0 {
				sb.WriteString(conjunction)
			}
			sb.WriteString(expression.ColumnExpression)
			if !inclusive {
				sb.WriteString(" NOT")
			}

			sb.WriteString(" LIKE ? ")
			textValue, ok := value.(string)
			if !ok {
				textValue = toolbox.AsString(value)
			}

			if !anyMatch {
				textValue = "%" + textValue + "%"
			}

			likeValues = append(likeValues, textValue)
		}
		c.addAll(likeValues...)

		if shouldPutBrackets || len(expression.Args) > 1 {
			sb.WriteString(")")
		}

	}

	return sb.String()
}
