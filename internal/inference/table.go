package inference

import (
	"context"
	"database/sql"
	"github.com/viant/sqlparser"
	"strings"
)

type (
	Table struct {
		Name      string
		Namespace string
		Columns   sqlparser.Columns
		index     map[string]*sqlparser.Column
		tables    []*Table
	}
)

func (t *Table) HasTable(table string) bool {
	if t.Name == table {
		return true
	}
	if len(t.tables) == 0 {
		return false
	}
	for _, candidate := range t.tables {
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
	if len(t.tables) == 0 {
		return false
	}
	for _, candidate := range t.tables {
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
		t.index = t.Columns.ByName()
	}
	if ret, ok := t.index[strings.ToLower(column)]; ok && (ns == "" || strings.ToLower(ns) == t.Namespace) {
		return ret
	}
	for _, table := range t.tables {
		if ret := table.lookup(ns, column); ret != nil {
			return ret
		}
	}
	return nil
}

func (t *Table) detect(ctx context.Context, db *sql.DB, SQL string) error {
	query, err := sqlparser.ParseQuery(SQL)
	if query == nil || query.From.X == nil {
		return err
	}
	t.Namespace = strings.ToLower(query.From.Alias)
	from := sqlparser.Stringify(query.From.X)
	if err = t.extractColumns(ctx, db, from); err != nil {
		return err
	}

	for _, join := range query.Joins {
		joinTable, err := NewTable(ctx, db, sqlparser.Stringify(join.With))
		if joinTable == nil {
			return err
		}
		joinTable.Namespace = strings.ToLower(join.Alias)
		t.tables = append(t.tables, joinTable)

	}
	return nil
}

func (t *Table) extractColumns(ctx context.Context, db *sql.DB, expr string) error {
	if !hasWhitespace(strings.TrimSpace(expr)) {
		expr = strings.Trim(expr, "`'")
		if index := strings.LastIndex(expr, "."); index != -1 {
			expr = expr[index+1:]
		}
		if sinkColumns, _ := readSinkColumns(ctx, db, expr); len(sinkColumns) > 0 {
			t.Columns = asColumns(sinkColumns)
		}
	} else if strings.Contains(strings.ToLower(expr), "select") {
		if err := t.detect(ctx, db, expr); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) detectColumns(ctx context.Context, db *sql.DB, table string) {
	if sinkColumns, _ := readSinkColumns(ctx, db, table); len(sinkColumns) > 0 {
		t.Columns = asColumns(sinkColumns)
	}
}

func NewTable(ctx context.Context, db *sql.DB, SQL string) (*Table, error) {
	table := &Table{}
	return table, table.detect(ctx, db, SQL)
}