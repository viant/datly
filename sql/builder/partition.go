package builder

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/sqlparser"
	sqltext "github.com/viant/sqlparser/source"
	"github.com/viant/sqlx"
)

// PartitionInput is the invocation-local SQL build input produced by a
// partitioner. Static partitioner identity/concurrency remain view metadata.
type PartitionInput struct {
	Table      string
	Expression string
	Args       []any
}

func (p *PartitionInput) Clone() *PartitionInput {
	if p == nil {
		return nil
	}
	return &PartitionInput{
		Table:      p.Table,
		Expression: p.Expression,
		Args:       append([]any(nil), p.Args...),
	}
}

func applyPartition(sqlText string, args []any, source *spec.ViewSource, partition *PartitionInput) (string, []any, error) {
	if partition == nil {
		return sqlText, args, nil
	}
	var err error
	if table := strings.TrimSpace(partition.Table); table != "" {
		if source == nil || strings.TrimSpace(source.Table) == "" {
			return "", nil, fmt.Errorf("partition table %q requires source table metadata", table)
		}
		sqlText, err = dsql.ReplaceTable(sqlText, source.Table, table)
		if err != nil {
			return "", nil, err
		}
	}
	expression := strings.TrimSpace(partition.Expression)
	if expression == "" {
		if len(partition.Args) > 0 {
			return "", nil, fmt.Errorf("partition arguments require an expression")
		}
		return sqlText, args, nil
	}
	if err := validatePartitionExpression(expression, len(partition.Args)); err != nil {
		return "", nil, err
	}
	withCriteria, insertAt := insertPartitionCriteria(sqlText, expression)
	argAt := positionalPlaceholderCount(sqlText[:insertAt])
	if argAt > len(args) {
		return "", nil, fmt.Errorf("partition placeholder position %d exceeds %d bound arguments", argAt, len(args))
	}
	resultArgs := make([]any, 0, len(args)+len(partition.Args))
	resultArgs = append(resultArgs, args[:argAt]...)
	resultArgs = append(resultArgs, partition.Args...)
	resultArgs = append(resultArgs, args[argAt:]...)
	return withCriteria, resultArgs, nil
}

func validatePartitionExpression(expression string, argCount int) error {
	parsed, err := sqlparser.ParseQuery("SELECT 1 FROM partition_source WHERE " + expression)
	if err != nil || parsed == nil || parsed.Qualify == nil || parsed.Qualify.X == nil {
		if err == nil {
			err = fmt.Errorf("empty partition expression")
		}
		return fmt.Errorf("invalid partition expression %q: %w", expression, err)
	}
	parameters := sqlx.ParseParameters(expression)
	if parameters.NamedCount() != 0 {
		return fmt.Errorf("partition expression must use positional placeholders")
	}
	if parameters.PositionalCount() != argCount {
		return fmt.Errorf("partition expression has %d placeholders but %d arguments", parameters.PositionalCount(), argCount)
	}
	return nil
}

func insertPartitionCriteria(sqlText, expression string) (string, int) {
	boundary := sqltext.CriteriaBoundary(sqlText)
	prefix := strings.TrimRight(sqlText[:boundary], " \t\r\n")
	clause := " WHERE (" + expression + ")"
	if sqltext.HasTopLevelClause(prefix, "where") {
		clause = " AND (" + expression + ")"
	}
	insertAt := len(prefix)
	result := insertRelationClause(sqlText, clause)
	return result, insertAt
}

func positionalPlaceholderCount(sqlText string) int {
	return sqlx.ParseParameters(sqlText).PositionalCount()
}
