package template

import (
	"fmt"
	"strings"

	"github.com/viant/datly/sql/fragment"
	sqlmacro "github.com/viant/datly/sql/macro"
	"github.com/viant/sqlx/metadata/info"
)

// ViewInput contains invocation-scoped values exposed through $View.
type ViewInput struct {
	Dialect               *info.Dialect
	TransactionActive     bool
	ParentValues          []any
	ParentCompositeValues [][]interface{}
	ExcludeParent         bool
	Limit                 int
	Offset                int
	Page                  int
	NonWindowSQL          string
	NonWindowArgs         []any
}

type viewContext struct {
	Limit             int
	Offset            int
	Page              int
	dialect           *info.Dialect
	transactionActive bool

	expander       sqlmacro.ParentKeyExpander
	bindings       *fragment.Bindings
	nonWindowSQL   string
	nonWindowArgs  []any
	parentExpanded bool
}

func newViewContext(input ViewInput, bindings *fragment.Bindings) *viewContext {
	return &viewContext{
		Limit:             input.Limit,
		Offset:            input.Offset,
		Page:              input.Page,
		dialect:           input.Dialect,
		transactionActive: input.TransactionActive,
		expander: sqlmacro.ParentKeyExpander{
			Dialect:       input.Dialect,
			ScalarValues:  input.ParentValues,
			CompositeRows: input.ParentCompositeValues,
			Exclude:       input.ExcludeParent,
		},
		bindings:      bindings,
		nonWindowSQL:  input.NonWindowSQL,
		nonWindowArgs: append([]any(nil), input.NonWindowArgs...),
	}
}

// ForUpdate renders the dialect's row-lock clause for an active transaction.
// SQLite relies on its transaction locking and has no SELECT row-lock clause.
func (v *viewContext) ForUpdate() (string, error) {
	if v == nil || !v.transactionActive {
		return "", fmt.Errorf("FOR UPDATE requires an active transaction")
	}
	if v.dialect == nil {
		return "", fmt.Errorf("FOR UPDATE requires a dialect")
	}
	switch strings.ToLower(strings.TrimSpace(v.dialect.Name)) {
	case "mysql", "postgresql", "postgres", "pg":
		return "FOR UPDATE", nil
	case "sqlite", "sqlite3":
		return "", nil
	default:
		return "", fmt.Errorf("FOR UPDATE is unsupported for dialect %q", v.dialect.Name)
	}
}

func (v *viewContext) NonWindowSQL() (string, error) {
	if strings.TrimSpace(v.nonWindowSQL) == "" {
		return "", fmt.Errorf("$View.NonWindowSQL requires a parent non-window query")
	}
	v.bindings.Append(v.nonWindowArgs...)
	return v.nonWindowSQL, nil
}

func (v *viewContext) ParentJoinOn(column string, prepend ...string) (string, error) {
	prefix := "AND"
	columns := []string{column}
	if len(prepend) > 0 {
		prefix = column
		columns = prepend
	}
	return v.expand(sqlmacro.ParentKeyCall{Method: "ParentJoinOn", Prefix: prefix, Columns: columns})
}

func (v *viewContext) ParentCompositeJoinOn(prefix string, columns ...string) (string, error) {
	return v.expand(sqlmacro.ParentKeyCall{Method: "ParentCompositeJoinOn", Prefix: prefix, Columns: columns})
}

func (v *viewContext) AndParentJoinOn(column string) (string, error) {
	return v.expand(sqlmacro.ParentKeyCall{Method: "AndParentJoinOn", Prefix: "AND", Columns: []string{column}})
}

func (v *viewContext) ColIn(prefix, column string) (string, error) {
	return v.expand(sqlmacro.ParentKeyCall{Method: "ColIn", Prefix: prefix, Columns: []string{column}})
}

func (v *viewContext) expand(call sqlmacro.ParentKeyCall) (string, error) {
	v.parentExpanded = true
	fragment, args, err := v.expander.Expand(call)
	if err != nil {
		return "", err
	}
	v.bindings.Append(args...)
	return fragment, nil
}

// TimestampSecondsUTC is an explicit dialect-rendered timestamp key.
func (v *viewContext) TimestampSecondsUTC(expression string) (string, error) {
	if v == nil {
		return "", fmt.Errorf("timestamp key requires view metadata")
	}
	return fragment.New(v.bindings).WithDialect(v.dialect).TimestampSecondsUTC(expression)
}

// TimestampNanoseconds keeps fractional ordering separate from whole seconds.
func (v *viewContext) TimestampNanoseconds(expression string) (string, error) {
	if v == nil {
		return "", fmt.Errorf("timestamp key requires view metadata")
	}
	return fragment.New(v.bindings).WithDialect(v.dialect).TimestampNanoseconds(expression)
}
