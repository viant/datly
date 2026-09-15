package readerbuilder

import (
	"fmt"
	"strings"

	"github.com/viant/datly/bootstrap/cacheconfig"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql"
	sqltext "github.com/viant/sqlparser/source"
)

func editView(source string, operation OperationType, mutation *ViewMutation) (string, error) {
	if mutation == nil || !validIdentifier(strings.TrimSpace(mutation.Name)) {
		return "", fmt.Errorf("view name is required and must be an identifier")
	}
	views, _, err := inspectViewSources(source)
	if err != nil {
		return "", err
	}
	index := -1
	for i := range views {
		if strings.EqualFold(views[i].Name, mutation.Name) {
			index = i
			break
		}
	}
	switch operation {
	case OperationAddView:
		if index >= 0 {
			return "", fmt.Errorf("view %q already exists", mutation.Name)
		}
		return addRelatedView(source, views, mutation)
	case OperationUpdateView:
		if index < 0 {
			return "", fmt.Errorf("view %q was not found", mutation.Name)
		}
		if strings.TrimSpace(mutation.SQL) == "" {
			return "", fmt.Errorf("view %q SQL is required", mutation.Name)
		}
		return dql.ApplyPatch(source, views[index].SourceSpan, strings.TrimSpace(mutation.SQL))
	case OperationRemoveView:
		if index < 0 {
			return "", fmt.Errorf("view %q was not found", mutation.Name)
		}
		if index == 0 {
			return "", fmt.Errorf("root view %q cannot be removed", mutation.Name)
		}
		return removeRelatedView(source, views[index])
	default:
		return "", fmt.Errorf("unsupported view operation %q", operation)
	}
}

func validateOperationResult(operation Operation, structure *Structure) error {
	if (operation.Type == OperationAddFieldPredicate || operation.Type == OperationUpdateFieldPredicate) && operation.Predicate != nil && structure != nil {
		intended := operation.Predicate.ExpansionViews
		if len(intended) == 0 {
			intended = []string{operation.Predicate.View}
		}
		seenTarget := false
		for _, site := range structure.PredicateExpansions {
			if site.Group != operation.Predicate.Group {
				continue
			}
			if !slicesContainsFold(intended, site.View) {
				return fmt.Errorf("predicate group %d unexpectedly expands in view %q after edit", site.Group, site.View)
			}
			seenTarget = seenTarget || strings.EqualFold(site.View, operation.Predicate.View)
		}
		if !seenTarget {
			return fmt.Errorf("predicate group %d is not expanded in target view %q after edit", operation.Predicate.Group, operation.Predicate.View)
		}
	}
	if operation.Type == OperationSetSetting && operation.Setting != nil && structure != nil && structure.Component != nil &&
		(strings.EqualFold(operation.Setting.Name, "cache") || strings.EqualFold(operation.Setting.Name, "cache_warmup")) {
		if settings := structure.Component.Settings; settings != nil && settings.Cache != nil && settings.Cache.Enabled {
			if strings.HasPrefix(strings.ToLower(strings.TrimSpace(settings.Cache.Provider)), "aerospike:") {
				return fmt.Errorf("inline Aerospike cache validation is unavailable; select a predefined service cache")
			}
			identity := structure.Component.Key.String() + ":reader-builder"
			if _, err := (cacheconfig.Config{Settings: settings.Cache, Identity: identity}).Validate(); err != nil {
				return fmt.Errorf("cache configuration: %w", err)
			}
		}
	}
	if operation.Type != OperationAddView || operation.View == nil || structure == nil || structure.Component == nil || structure.Component.RootView == nil {
		return nil
	}
	parent := strings.TrimSpace(operation.View.Parent)
	if parent == "" {
		parent = structure.Component.RootView.Namespace
	}
	relation := findRelation(structure.Component.RootView, operation.View.Name)
	if relation == nil {
		return fmt.Errorf("added view %q did not compile as a relation", operation.View.Name)
	}
	if !strings.EqualFold(strings.TrimSpace(relation.ParentNamespace), parent) {
		return fmt.Errorf("added view %q resolved parent %q, expected %q", operation.View.Name, relation.ParentNamespace, parent)
	}
	return nil
}

func findRelation(view *spec.View, name string) *spec.Relation {
	if view == nil {
		return nil
	}
	for _, relation := range view.Relations {
		if relation == nil {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(relation.Name), strings.TrimSpace(name)) {
			return relation
		}
		if found := findRelation(relation.View, name); found != nil {
			return found
		}
	}
	return nil
}

func addRelatedView(source string, views []ViewOccurrence, mutation *ViewMutation) (string, error) {
	if len(views) == 0 {
		return "", fmt.Errorf("addView requires an existing wrapped root view")
	}
	if strings.TrimSpace(mutation.SQL) == "" || strings.TrimSpace(mutation.On) == "" {
		return "", fmt.Errorf("addView requires SQL and relation ON expression")
	}
	parent := strings.TrimSpace(mutation.Parent)
	if parent == "" {
		parent = views[0].Name
	}
	if !containsView(views, parent) {
		return "", fmt.Errorf("parent view %q was not found", parent)
	}
	join := strings.ToUpper(strings.TrimSpace(mutation.Join))
	switch join {
	case "", "JOIN":
		join = "JOIN"
	case "LEFT", "LEFT JOIN":
		join = "LEFT JOIN"
	case "LEFT OUTER", "LEFT OUTER JOIN":
		join = "LEFT OUTER JOIN"
	case "RIGHT", "RIGHT JOIN":
		join = "RIGHT JOIN"
	case "RIGHT OUTER", "RIGHT OUTER JOIN":
		join = "RIGHT OUTER JOIN"
	case "INNER", "INNER JOIN":
		join = "INNER JOIN"
	case "FULL", "FULL JOIN":
		join = "FULL JOIN"
	case "FULL OUTER", "FULL OUTER JOIN":
		join = "FULL OUTER JOIN"
	default:
		return "", fmt.Errorf("join %q is unsupported", mutation.Join)
	}
	projection, err := outerProjection(source)
	if err != nil {
		return "", err
	}
	prepared := dql.PrepareSource(source)
	statement := prepared.Statements[0]
	statementStart := prepared.TrimPrefix + statement.SQLStart
	statementEnd := prepared.TrimPrefix + statement.SQLEnd
	outer := source[statementStart:statementEnd]
	boundary := statementStart + sqltext.CriteriaBoundary(outer)
	patches := []sourcePatch{
		{span: dql.SourceSpan{Start: projection.end, End: projection.end}, text: ", " + mutation.Name + ".* "},
		{span: dql.SourceSpan{Start: boundary, End: boundary}, text: fmt.Sprintf("\n%s (%s) %s ON %s\n", join, strings.TrimSpace(mutation.SQL), mutation.Name, strings.TrimSpace(mutation.On))},
	}
	return applySourcePatches(source, patches)
}

func removeRelatedView(source string, view ViewOccurrence) (string, error) {
	projection, err := outerProjection(source)
	if err != nil {
		return "", err
	}
	var projectionSpan *dql.SourceSpan
	want := strings.ToLower(strings.TrimSpace(view.Name)) + ".*"
	for _, span := range projection.items {
		if strings.EqualFold(strings.Join(strings.Fields(source[span.Start:span.End]), ""), want) {
			copy := span
			projectionSpan = &copy
			break
		}
	}
	if projectionSpan == nil {
		return "", fmt.Errorf("view %q projection was not found", view.Name)
	}
	prepared := dql.PrepareSource(source)
	statement := prepared.Statements[0]
	statementStart := prepared.TrimPrefix + statement.SQLStart
	statementEnd := prepared.TrimPrefix + statement.SQLEnd
	outer := source[statementStart:statementEnd]
	viewOpen := view.SourceSpan.Start - statementStart - 1
	joinStart := -1
	for position := sqltext.FindTopLevelKeyword(outer, "join", 0); position >= 0; position = sqltext.FindTopLevelKeyword(outer, "join", position+4) {
		if position < viewOpen {
			joinStart = position
		}
	}
	if joinStart < 0 {
		return "", fmt.Errorf("view %q relation JOIN was not found", view.Name)
	}
	joinKeyword := joinStart
	joinStart = includeJoinModifier(outer, joinStart)
	joinEnd := sqltext.CriteriaBoundary(outer)
	if next := sqltext.FindTopLevelKeyword(outer, "join", joinKeyword+4); next >= 0 && next < joinEnd {
		joinEnd = includeJoinModifier(outer, next)
	}
	return applySourcePatches(source, []sourcePatch{
		{span: removalSpan(source, projection, *projectionSpan)},
		{span: dql.SourceSpan{Start: statementStart + joinStart, End: statementStart + joinEnd}},
	})
}

func includeJoinModifier(source string, joinStart int) int {
	firstStart, first := previousSQLWord(source, joinStart)
	if first == "outer" {
		secondStart, second := previousSQLWord(source, firstStart)
		switch second {
		case "left", "right", "full":
			return secondStart
		}
	}
	switch first {
	case "left", "right", "inner", "full", "cross", "natural":
		return firstStart
	}
	return joinStart
}

func previousSQLWord(source string, before int) (int, string) {
	end := before
	for end > 0 && sqltext.IsWhitespace(source[end-1]) {
		end--
	}
	start := end
	for start > 0 {
		ch := source[start-1]
		if (ch < 'a' || ch > 'z') && (ch < 'A' || ch > 'Z') {
			break
		}
		start--
	}
	return start, strings.ToLower(source[start:end])
}

func containsView(views []ViewOccurrence, name string) bool {
	for _, view := range views {
		if strings.EqualFold(view.Name, name) {
			return true
		}
	}
	return false
}
