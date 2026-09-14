package builder

import (
	"fmt"
	"strings"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	sqlmacro "github.com/viant/datly/sql/macro"
)

// PreparedRelationSQL resolves the authored SQL text for a prepared relation
// query from the derived runtime relation, including $View.NonWindowSQL
// expansion. Placeholder binding stays outside this owner: route preparation
// still binds named placeholders against the already-bound input after this
// relation-owned SQL text is resolved.
func PreparedRelationSQL(component *spec.Component, relation *data.Relation, rootNonWindowSQL string) (string, error) {
	if !preparedRelationQueryCandidate(relation) {
		return "", nil
	}
	sqlText := NormalizePreparedRelationSQL(relation.Of.View.Spec.Source.SQL)
	if sqlText == "" {
		return "", nil
	}
	if err := validatePreparedRelationAliases(component, relation, sqlText); err != nil {
		return "", err
	}
	sqlText, matched := sqlmacro.ExpandNonWindowSQL(sqlText, rootNonWindowSQL, preparedRelationAliases(component)...)
	if matched > 0 && strings.TrimSpace(rootNonWindowSQL) == "" {
		return "", MissingPreparedRelationRootSQL(component, relation)
	}
	return sqlText, nil
}

func validatePreparedRelationAliases(component *spec.Component, relation *data.Relation, sqlText string) error {
	if err := sqlmacro.ValidateNonWindowSQLAliases(sqlText, preparedRelationAliases(component)...); err != nil {
		relationKey := preparedRelationQueryKey(relation)
		if relationKey == "" {
			relationKey = "<unnamed>"
		}
		return fmt.Errorf("prepared relation query %s: %w", relationKey, err)
	}
	return nil
}

func NormalizePreparedRelationSQL(sqlText string) string {
	sqlText = strings.TrimSpace(sqlText)
	sqlText = strings.TrimPrefix(sqlText, "?")
	return strings.TrimSpace(sqlText)
}

func MissingPreparedRelationRootSQL(component *spec.Component, relation *data.Relation) error {
	componentName := ""
	if component != nil {
		componentName = component.Name
	}
	relationKey := preparedRelationQueryKey(relation)
	if relationKey == "" {
		relationKey = "<unnamed>"
	}
	return fmt.Errorf("component %s prepared relation query %s references $View.NonWindowSQL but root SQL is empty", componentName, relationKey)
}

func preparedRelationQueryCandidate(relation *data.Relation) bool {
	if relation == nil || relation.Of == nil || relation.Of.View == nil || relation.Of.View.Spec.Source == nil {
		return false
	}
	if relation.Kind != spec.RelationKindDerived {
		return false
	}
	if strings.TrimSpace(relation.Name) == "" && strings.TrimSpace(relation.Holder) == "" {
		return false
	}
	if relation.Cardinality != spec.CardinalityOne || relation.Of.MatchStrategy != data.MatchSequential {
		return false
	}
	if len(relation.On) != 0 || len(relation.Of.On) != 0 {
		return false
	}
	return strings.TrimSpace(relation.Of.View.Spec.Source.SQL) != ""
}

func preparedRelationQueryKey(relation *data.Relation) string {
	if relation == nil {
		return ""
	}
	if holder := strings.TrimSpace(relation.Holder); holder != "" {
		return holder
	}
	return strings.TrimSpace(relation.Name)
}

func PreparedRelationNonWindowTokens(component *spec.Component) []string {
	return preparedRelationNonWindowTokens(component)
}

func preparedRelationNonWindowTokens(component *spec.Component) []string {
	return sqlmacro.NonWindowSQLTokens(preparedRelationAliases(component)...)
}

func preparedRelationAliases(component *spec.Component) []string {
	if component == nil {
		return nil
	}
	var result []string
	if component.RootView != nil {
		if name := strings.TrimSpace(component.RootView.Name); name != "" {
			result = append(result, name)
		}
	}
	if name := strings.TrimSpace(component.Name); name != "" {
		for _, candidate := range result {
			if candidate == name {
				return result
			}
		}
		result = append(result, name)
	}
	return result
}
