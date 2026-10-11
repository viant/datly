package builder

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
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
	scopeStart, scopeEnd, err := partitionCriteriaScope(sqlText, expression)
	if err != nil {
		return "", nil, err
	}
	scoped, insertAt := insertPartitionCriteria(sqlText[scopeStart:scopeEnd], expression)
	withCriteria := sqlText[:scopeStart] + scoped + sqlText[scopeEnd:]
	insertAt += scopeStart
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

// A named-source wrapper does not expose its inner table aliases. Keep the
// partition predicate in the SELECT that declares those aliases, without
// flattening the source or moving its other predicates and controls.
func partitionCriteriaScope(sqlText, expression string) (int, int, error) {
	parsed, err := sqlparser.ParseQuery("SELECT 1 FROM partition_source WHERE " + expression)
	if err != nil {
		return 0, 0, err
	}
	namespaces := map[string]bool{}
	hasSubquery := false
	sqlparser.Traverse(parsed.Qualify.X, func(candidate node.Node) bool {
		if _, nested := candidate.(*query.Select); nested {
			hasSubquery = true
			return false
		}
		if selector, ok := candidate.(*expr.Selector); ok {
			namespaces[strings.ToLower(sqltext.TrimQuote(selector.Name))] = true
		}
		return true
	})
	// Correlated subqueries own additional scopes. Retain the established
	// outer placement rather than treating their local aliases as root aliases.
	if len(namespaces) == 0 || hasSubquery {
		return 0, len(sqlText), nil
	}
	start, end := 0, len(sqlText)
	for depth := 0; depth < 32; depth++ {
		text := sqlText[start:end]
		statement, err := sqlparser.ParseQuery(text)
		if err != nil {
			return 0, 0, fmt.Errorf("parse partition source: %w", err)
		}
		visible := map[string]bool{}
		addSource := func(alias string, source node.Node) {
			if alias == "" {
				if table, _, err := sqlparser.SourceTable(source); err == nil {
					if parts, err := sqlparser.TableIdentifierParts(table); err == nil && len(parts) > 0 {
						alias = parts[len(parts)-1]
					}
				}
			}
			visible[strings.ToLower(sqltext.TrimQuote(alias))] = true
		}
		addSource(statement.From.Alias, statement.From.X)
		for _, join := range statement.Joins {
			addSource(join.Alias, join.With)
		}
		allVisible := true
		for namespace := range namespaces {
			allVisible = allVisible && visible[namespace]
		}
		if allVisible {
			return start, end, nil
		}
		raw, nested := statement.From.X.(*expr.Raw)
		if !nested || statement.Union != nil {
			break
		}
		// The existing parser retains the exact derived-source spelling.
		// Its source scanner locates code only, excluding literals/comments.
		at := sqltext.FindCodeToken(text, raw.Raw, 0)
		if at < 0 {
			break
		}
		group, after, ok := sqltext.ReadGroupString(text, at, '(', ')')
		if !ok || group != raw.Raw {
			break
		}
		end = start + after - 1
		start += at + 1
	}
	return 0, 0, fmt.Errorf("partition expression namespaces are not visible in one source scope: %s", expression)
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
