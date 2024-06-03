package expand

import (
	"database/sql"
	"github.com/viant/datly/utils/types"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
)

const (
	ExecTypeInsert ExecType = iota
	ExecTypeUpdate
	ExecTypeDelete
)

type (
	Expander interface {
		ColIn(prefix, column string) (string, error)
		In(prefix string) (string, error)
		ParentJoinOn(column string, prepend ...string) (string, error)
		AndParentJoinOn(column string) (string, error)
	}

	ParentSource interface {
		Dber
		ViewName() string
		TableAlias() string
		TableName() string
		ResultLimit() int
	}

	Dber interface {
		Db() (*sql.DB, error)
	}

	ParentExtras interface {
		CurrentLimit() int
		CurrentOffset() int
		CurrentPage() int
	}

	ParentBatch interface {
		ColIn() []interface{}
		ColInBatch() []interface{}
	}

	ViewContext struct {
		Name         string
		Alias        string
		Table        string
		Limit        int
		Offset       int
		Page         int
		Args         []interface{}
		NonWindowSQL string
		ParentValues []interface{}

		expander Expander  `velty:"-"`
		DataUnit *DataUnit `velty:"-"`
	}

	Executable struct {
		Table    string
		ExecType ExecType
		Data     interface{}
		IsLast   bool
		executed bool
	}

	ExecType     int
	MockExpander struct{}
)

func (e *Executable) Executed() bool {
	return e.executed
}

func (e *Executable) MarkAsExecuted() {
	e.executed = true
}

func (e *MockExpander) ParentJoinOn(column string, prepend ...string) (string, error) {
	return "", nil
}

func (e *MockExpander) AndParentJoinOn(column string) (string, error) {
	return e.ColIn("", column)
}

func (e *MockExpander) ColIn(prefix, column string) (string, error) {
	return "", nil
}

func (e *MockExpander) In(prefix string) (string, error) {
	return "", nil
}

func (m *ViewContext) ParentJoinOn(column string, prepend ...string) (string, error) {
	if len(prepend) > 0 {
		return m.ColIn(column, prepend[0])
	}
	return m.ColIn("AND", column)
}

func (m *ViewContext) AndParentJoinOn(column string) (string, error) {
	return m.ColIn("AND", column)
}

func (m *ViewContext) ColIn(prefix, column string) (string, error) {
	if m.expander != nil {
		return m.expander.ColIn(prefix, column)
	}

	bindings := m.addBindings(m.ParentValues)
	if bindings == "" {
		return prefix + " 1 = 0 ", nil
	}

	if prefix != "" && !strings.HasSuffix(prefix, " ") {
		prefix = prefix + " "
	}

	return prefix + column + " IN (" + bindings + " )", nil
}

func (m *ViewContext) addBindings(args []interface{}) string {
	_, bindings := AsBindings("", args)
	m.DataUnit.addAll(args...)
	return bindings
}

func (m *ViewContext) In(prefix string) (string, error) {
	return m.ColIn(prefix, "")
}

// AddRelation appends SQL and adds binding arguments
// Deprecated: For the backward compatibility
func (m *ViewContext) Expand(_ *DataUnit) string {
	m.DataUnit.addAll(m.Args...)
	return m.NonWindowSQL
}

func AsBindings(key string, values []interface{}) (column string, bindings string) {
	switch len(values) {
	case 0:
		return "", ""
	case 1:
		return key, "?"
	case 2:
		return key, "?, ?"
	case 3:
		return key, "?, ?, ?"
	case 4:
		return key, "?, ?, ?, ?"
	default:
		sb := strings.Builder{}
		sb.WriteByte('?')
		for i := 1; i < len(values); i++ {
			sb.WriteString(", ?")
		}
		return key, sb.String()
	}
}

func NewViewContext(metaSource ParentSource, aSelector ParentExtras, batchData ParentBatch, options ...interface{}) *ViewContext {
	if metaSource == nil {
		return nil
	}

	var sanitizer *DataUnit
	var expander Expander
	var colInArgs []interface{}

	for _, option := range options {
		switch actual := option.(type) {
		case *DataUnit:
			sanitizer = actual
		case Expander:
			expander = actual
		}
	}

	if batchData != nil {
		colInArgs = batchData.ColInBatch()
	}
	limit := metaSource.ResultLimit()
	offset := 0
	page := 0

	if aSelector != nil {
		limit = NotZeroOf(aSelector.CurrentLimit(), limit)
		offset = NotZeroOf(aSelector.CurrentOffset(), offset)
		page = NotZeroOf(aSelector.CurrentPage(), 0)
	}

	var args []interface{}
	var SQLExec string
	if sanitizer != nil {
		args = sanitizer.ParamsGroup
		SQLExec = sanitizer.TemplateSQL
	}
	result := &ViewContext{
		expander:     expander,
		Name:         metaSource.ViewName(),
		Alias:        metaSource.TableAlias(),
		Table:        metaSource.TableName(),
		Limit:        limit,
		Page:         page,
		Offset:       offset,
		Args:         args,
		NonWindowSQL: SQLExec,
		DataUnit:     NewDataUnit(metaSource),
		ParentValues: colInArgs,
	}

	return result
}

func NewDataUnit(metaSource Dber) *DataUnit {
	return &DataUnit{
		MetaSource: metaSource,
		Statements: NewStmtHolder(),
	}
}

func NotZeroOf(values ...int) int {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}

	return 0
}

func (c *DataUnit) Insert(data interface{}, tableName string) (string, error) {
	return c.Statements.InsertWithMarker(tableName, data), nil
}

func (c *DataUnit) Update(data interface{}, tableName string) (string, error) {
	return c.Statements.UpdateWithMarker(tableName, data), nil
}

func (i ExecutablesIndex) UpdateLastExecutable(execType ExecType, tableName string, newExecutable *Executable) {
	if execType == ExecTypeInsert {
		if lastExecutable, ok := i[tableName]; ok {
			lastExecutable.IsLast = false
		}

		i[tableName] = newExecutable
	}
}

func copyValues(data []interface{}) []interface{} {
	result := make([]interface{}, 0, len(data))
	for _, datum := range data {
		result = append(result, copyValue(datum))
	}
	return result
}

func copyValue(data interface{}) interface{} {
	switch actual := data.(type) {
	case string:
		return actual
	case int:
		return actual
	case int64:
		return actual
	case uint64:
		return actual
	case float32:
		return actual
	case float64:
		return actual
	case uint:
		return actual
	case bool:
		return actual
	case int8:
		return actual
	case uint8:
		return actual
	case int32:
		return actual
	case uint32:
		return actual
	case int16:
		return actual
	case uint16:
		return actual
	}

	result := reflect.ValueOf(data)
	switch result.Kind() {
	case reflect.Slice:
		sliceResult := reflect.MakeSlice(result.Type(), result.Len(), result.Len())
		reflect.Copy(sliceResult, result)
		return sliceResult.Interface()
	default:
		dest := types.NewValue(result.Type())
		actualType := result.Type()
		if actualType.Kind() == reflect.Ptr {
			actualType = actualType.Elem()
		}

		xunsafe.Copy(xunsafe.AsPointer(dest), xunsafe.AsPointer(data), int(actualType.Size()))
		return dest
	}
}
