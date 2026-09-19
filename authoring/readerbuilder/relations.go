package readerbuilder

import (
	"fmt"
	"strings"

	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/sqlparser"
)

func updateRelation(source string, mutation *RelationMutation) (string, error) {
	if mutation == nil || !validIdentifier(strings.TrimSpace(mutation.Name)) || !validIdentifier(strings.TrimSpace(mutation.Parent)) || strings.TrimSpace(mutation.On) == "" {
		return "", fmt.Errorf("relation name, parent, and ON expression are required")
	}
	prepared := dql.PrepareSource(source)
	if err := prepared.Err(); err != nil {
		return "", err
	}
	if len(prepared.Statements) != 1 || prepared.Statements[0] == nil {
		return "", fmt.Errorf("updateRelation requires exactly one DQL statement")
	}
	statement := prepared.Statements[0]
	start := prepared.TrimPrefix + statement.SQLStart
	end := prepared.TrimPrefix + statement.SQLEnd
	outer := source[start:end]
	parsed, err := sqlparser.ParseQuery(maskTemplateExpressions(outer), sqlparser.WithStructuralValidation())
	if err != nil {
		return "", err
	}
	for _, join := range parsed.Joins {
		if join == nil || !strings.EqualFold(strings.TrimSpace(join.Alias), strings.TrimSpace(mutation.Name)) {
			continue
		}
		if join.OnSpan.End <= join.OnSpan.Begin {
			return "", fmt.Errorf("relation %q has no editable ON expression", mutation.Name)
		}
		span := dql.SourceSpan{Start: start + int(join.OnSpan.Begin), End: start + int(join.OnSpan.End)}
		return dql.ApplyPatch(source, span, "ON "+strings.TrimSpace(mutation.On))
	}
	return "", fmt.Errorf("relation %q was not found", mutation.Name)
}
