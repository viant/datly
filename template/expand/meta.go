package expand

import (
	"database/sql"
	"strings"
)

type (
	Expander interface {
		ColIn(prefix, column string) (string, error)
		In(prefix string) (string, error)
		ParentJoinOn(column string, prepend ...string) (string, error)
		AndParentJoinOn(column string) (string, error)
	}

	MetaSource interface {
		ViewName() string
		TableAlias() string
		TableName() string
		ResultLimit() int
		Db() (*sql.DB, error)
	}

	MetaExtras interface {
		CurrentLimit() int
		CurrentOffset() int
		CurrentPage() int
	}

	MetaBatch interface {
		ColIn() []interface{}
		ColInBatch() []interface{}
	}

	MetaParam struct {
		expander     Expander
		sanitizer    *SQLCriteria
		Name         string
		Alias        string
		Table        string
		Limit        int
		Offset       int
		Page         int
		Args         []interface{}
		NonWindowSQL string
		ParentValues []interface{}
	}

	MockExpander struct{}
)

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

func (m *MetaParam) ParentJoinOn(column string, prepend ...string) (string, error) {
	if len(prepend) > 0 {
		return m.ColIn(column, prepend[0])
	}
	return m.ColIn("AND", column)
}

func (m *MetaParam) AndParentJoinOn(column string) (string, error) {
	return m.ColIn("AND", column)
}

func (m *MetaParam) ColIn(prefix, column string) (string, error) {
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

func (m *MetaParam) addBindings(args []interface{}) string {
	_, bindings := AsBindings("", args)
	m.sanitizer.addAll(args...)
	return bindings
}

func (m *MetaParam) In(prefix string) (string, error) {
	return m.ColIn(prefix, "")
}

//For the backward compatibility
func (m *MetaParam) Expand(_ *SQLCriteria) string {
	m.sanitizer.addAll(m.Args...)
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

func NewMetaParam(metaSource MetaSource, aSelector MetaExtras, batchData MetaBatch, options ...interface{}) *MetaParam {
	if metaSource == nil {
		return nil
	}

	var sanitizer *SQLCriteria
	var expander Expander
	var colInArgs []interface{}

	for _, option := range options {
		switch actual := option.(type) {
		case *SQLCriteria:
			sanitizer = actual
		case Expander:
			expander = actual
		}
	}

	if batchData != nil {
		colInArgs = batchData.ColIn()
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

	viewParam := &MetaParam{
		expander:     expander,
		Name:         metaSource.ViewName(),
		Alias:        metaSource.TableAlias(),
		Table:        metaSource.TableName(),
		Limit:        limit,
		Page:         page,
		Offset:       offset,
		Args:         args,
		NonWindowSQL: SQLExec,
		sanitizer: &SQLCriteria{
			MetaSource: metaSource,
		},
		ParentValues: colInArgs,
	}

	return viewParam
}

func NotZeroOf(values ...int) int {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}

	return 0
}

func MockMetaParam() *MetaParam {
	return &MetaParam{
		sanitizer: &SQLCriteria{},
	}
}
