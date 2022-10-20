package columns

import (
	"context"
	"database/sql"
	"github.com/viant/datly/reader/metadata"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/parsly/matcher"
	"github.com/viant/sqlx/io"
	rdata "github.com/viant/toolbox/data"
	"strings"
)

const selectKeyword = "select"

func DetectColumns(ctx context.Context, db *sql.DB, query string, args ...interface{}) ([]io.Column, error) {
	selectStmt := ExpandWithFalseCondition(ensureSelect(query))
	rows, err := db.QueryContext(ctx, selectStmt, args...)
	defer func() {
		if rows != nil {
			_ = rows.Close()
		}
	}()

	if err != nil {
		return nil, err
	}

	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	ioColumns := io.TypesToColumns(types)
	return ioColumns, nil
}

func ensureSelect(query string) string {
	if !ContainsSelect(query) {
		sb := strings.Builder{}
		sb.WriteString("SELECT ")
		sb.WriteString("* FROM ")
		sb.WriteString(query)
		sb.WriteString(" ")
		return sb.String()
	}

	return query
}

func isSelectKeyword(query string, index int) bool {
	if index == -1 {
		return false
	}

	if index != 0 && (!matcher.IsWhiteSpace(query[index-1]) && query[index-1] != '(') {
		return false
	}

	index += len(selectKeyword)
	if index < len(query)-1 && !matcher.IsWhiteSpace(query[index]) && query[index] != '(' {
		return false
	}

	return true
}

func ExpandWithFalseCondition(source string) string {
	discover := metadata.EnrichWithDiscover(source, false)
	replacement := rdata.Map{}
	replacement.Put(keywords.AndCriteria[1:], "\n\n AND 1=0 ")
	replacement.Put(keywords.WhereCriteria[1:], "\n\n WHERE 1=0 ")
	replacement.Put(keywords.Pagination[1:], "")
	SQL := replacement.ExpandAsText(discover)
	return SQL
}

func ContainsSelect(query string) bool {
	queryLower := strings.ToLower(query)
	selectIndex := strings.Index(queryLower, selectKeyword)

	return isSelectKeyword(queryLower, selectIndex)
}

func CanBeTableName(candidate string) bool {
	return len(strings.Fields(candidate)) == 1
}
