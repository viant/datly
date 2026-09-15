package compile

import (
	"fmt"
	"strings"

	"github.com/viant/datly/tag"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
)

// validateNestedViewSQL keeps Datly annotations on the outer named-view graph.
// Nested statements are database SQL and are inspected without rewriting them.
func validateNestedViewSQL(parsed *query.Select) error {
	if parsed == nil {
		return nil
	}
	for _, with := range parsed.WithSelects {
		if with == nil {
			continue
		}
		if with.X != nil {
			if err := validateDatabaseSQL(with.X); err != nil {
				return err
			}
		} else if err := validateDatabaseSource(&expr.Raw{Raw: with.Raw}); err != nil {
			return err
		}
	}
	if err := validateDatabaseSource(parsed.From.X); err != nil {
		return err
	}
	for _, join := range parsed.Joins {
		if join != nil {
			if err := validateDatabaseSource(join.With); err != nil {
				return err
			}
		}
	}
	if parsed.Union != nil {
		return validateDatabaseSQL(parsed.Union.X)
	}
	return nil
}

func validateDatabaseSource(source node.Node) error {
	if table, _, err := sqlparser.SourceTable(source); err != nil {
		return err
	} else if table != "" {
		return nil
	}
	var raw string
	switch actual := source.(type) {
	case *expr.Raw:
		raw = actual.Raw
	case *expr.Parenthesis:
		raw = actual.Raw
	case *query.Select:
		return validateDatabaseSQL(actual)
	default:
		return nil
	}
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	prepared, err := prepareSubquery(raw)
	if err != nil {
		return &Error{Code: CodeSQLParse, Cause: fmt.Errorf("parse nested view source: %w", err)}
	}
	if prepared == nil {
		return nil
	}
	return validateDatabaseSQL(prepared.query)
}

func validateDatabaseSQL(parsed *query.Select) error {
	if parsed == nil {
		return nil
	}
	if containsViewDirective(parsed) || containsSQLCall(parsed, func(name string) bool { return name == tag.InvariantName || name == "tag" }) {
		return &Error{Code: CodeViewDirective, Cause: fmt.Errorf("Datly view controls, tag and invariant annotations belong on the outer view projection, not inside database SQL")}
	}
	return validateNestedViewSQL(parsed)
}

type preparedSubquery struct {
	query  *query.Select
	embeds []string
}

func prepareSubquery(source string) (*preparedSubquery, error) {
	trimmed := strings.TrimSpace(source)
	wrapped := len(trimmed) >= 2 && trimmed[0] == '(' && trimmed[len(trimmed)-1] == ')'
	if wrapped {
		trimmed = strings.TrimSpace(trimmed[1 : len(trimmed)-1])
	}
	refs := dql.EmbeddedSQLRefs(trimmed)
	if len(refs) == 1 && strings.TrimSpace(trimmed) == refs[0].Raw {
		return nil, nil
	}
	prepared := &preparedSubquery{}
	for index, ref := range refs {
		if ref == nil || ref.Raw == "" {
			continue
		}
		placeholder := embedPlaceholderName(trimmed, index)
		trimmed = strings.ReplaceAll(trimmed, ref.Raw, placeholder)
		prepared.embeds = append(prepared.embeds, placeholder)
	}
	parsed, err := parseReadSQL(trimmed)
	if err != nil {
		return nil, err
	}
	if parsed == nil {
		return nil, fmt.Errorf("nested source is not a SELECT query")
	}
	prepared.query = parsed
	return prepared, nil
}

func embedPlaceholderName(source string, index int) string {
	for {
		candidate := fmt.Sprintf("__datly_embed_ref_%d__", index)
		if !strings.Contains(source, candidate) {
			return candidate
		}
		index++
	}
}
