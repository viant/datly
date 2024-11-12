package inference

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/view/column"
	"github.com/viant/sqlparser"
	"strings"
)

type (
	Table struct {
		Name           string
		Namespace      string
		Columns        sqlparser.Columns
		QueryColumns   sqlparser.Columns
		index          map[string]*sqlparser.Column
		Tables         []*Table
		OutputJSONHint string
	}
)

func (t *Table) HasTable(table string) bool {
	if t.Name == table {
		return true
	}
	if len(t.Tables) == 0 {
		return false
	}
	for _, candidate := range t.Tables {
		if candidate.HasTable(table) {
			return true
		}
	}
	return false
}

func (t *Table) HasNamespace(ns string) bool {
	if t.Namespace == ns {
		return true
	}
	if len(t.Tables) == 0 {
		return false
	}
	for _, candidate := range t.Tables {
		if candidate.HasTable(ns) {
			return true
		}
	}
	return false
}

func (t *Table) Lookup(column string) *sqlparser.Column {
	if index := strings.Index(column, "."); index != -1 {
		return t.lookup(column[:index], column[index+1:])
	}
	return t.lookup("", column)
}

func (t *Table) lookup(ns, column string) *sqlparser.Column {
	if len(t.index) == 0 {
		t.index = t.Columns.ByLowerCase()
	}
	if ret, ok := t.index[strings.ToLower(column)]; ok && (ns == "" || strings.ToLower(ns) == t.Namespace) {
		return ret
	}
	for _, table := range t.Tables {
		if ret := table.lookup(ns, column); ret != nil {
			return ret
		}
	}
	return nil
}

func (t *Table) detect(ctx context.Context, db *sql.DB, SQL string) error {
	SQL = TrimParenthesis(SQL)
	query, err := sqlparser.ParseQuery(SQL)
	if err != nil && query.From.X == nil { //TODO add velty expr  handler
		return fmt.Errorf("unable to parseSQL to detect table: %w", err)
	}
	query, err = column.RewriteWithQueryIfNeeded(SQL, query)
	if query == nil || query.From.X == nil {
		if query != nil && len(query.List) > 0 {
			t.Columns = sqlparser.NewColumns(query.List)
		}
		return err
	}
	if !query.List.IsStarExpr() {
		t.QueryColumns = sqlparser.NewColumns(query.List)
		t.Columns = t.QueryColumns
	}
	t.Namespace = strings.ToLower(query.From.Alias)
	from := sqlparser.Stringify(query.From.X)

	trimFrom := strings.TrimSpace(from)
	if strings.HasPrefix(trimFrom, "(") && strings.HasSuffix(trimFrom, ")") {
		from = trimFrom[1 : len(trimFrom)-1]
	}
	if !HasWhitespace(from) {
		t.Name = from
	}
	if err = t.extractColumns(ctx, db, from); err != nil {
		if len(t.Columns) == 0 { //no extracted column with db driver error
			return err
		}
	}
	for _, join := range query.Joins {
		joinTable, err := NewTable(ctx, db, sqlparser.Stringify(join.With))
		if joinTable == nil {
			return err
		}
		joinTable.Namespace = strings.ToLower(join.Alias)
		t.Tables = append(t.Tables, joinTable)
	}
	setter.SetStringIfEmpty(&t.OutputJSONHint, query.From.Comments)
	return nil
}

func (t *Table) Detect(ctx context.Context, db *sql.DB) (err error) {
	SQL := "SELECT * FROM " + t.Name + " WHERE 1 = 1"
	t.Columns, err = column.Discover(ctx, db, t.Name, SQL)
	return err
}

func (t *Table) AppendTable(table *Table) {
	t.Tables = append(t.Tables, table)
}

func (t *Table) extractColumns(ctx context.Context, db *sql.DB, expr string) (err error) {
	if !HasWhitespace(strings.TrimSpace(expr)) {
		expr = strings.Trim(expr, "`'")
		if index := strings.LastIndex(expr, "."); index != -1 {
			expr = expr[index+1:]
		}
		t.Columns, err = column.Discover(ctx, db, expr, "SELECT * FROM "+expr+" WHERE 1 = 0")

	} else if strings.Contains(strings.ToLower(expr), "select") {
		t.Columns, err = column.Discover(ctx, db, "", expr)
	}
	return err
}

func (t *Table) detectColumns(ctx context.Context, db *sql.DB, table string) {
	t.Columns, _ = column.Discover(ctx, db, table, "SELECT * FROM "+table+" WHERE 1 = 0")
}

func NewTable(ctx context.Context, db *sql.DB, SQL string) (*Table, error) {
	table := &Table{}
	return table, table.detect(ctx, db, SQL)
}
