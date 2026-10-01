package sql

import (
	"github.com/viant/datly/data"
	"github.com/viant/sqlx/metadata/info"
)

// generatedColumnExpression quotes only generated identifier references.
// Authored expressions and fallback literals remain SQL owned by the author.
func generatedColumnExpression(column *data.Column, allowNulls bool, dialect *info.Dialect) (string, error) {
	if dialect == nil {
		return column.SelectExpression(allowNulls), nil
	}
	copied := *column
	var err error
	copied.Column, err = dialect.ColumnIdentifier(copied.Column)
	if err != nil {
		return "", err
	}
	copied.Name, err = dialect.ColumnIdentifier(copied.Name)
	if err != nil {
		return "", err
	}
	return copied.SelectExpression(allowNulls), nil
}
