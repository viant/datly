package readerbuilder

import (
	"fmt"
	"strings"

	"github.com/viant/datly/bootstrap/cacheconfig"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql"
	sqltext "github.com/viant/sqlparser/source"
	"github.com/viant/tagly/format/text"
)

func editView(source string, operation OperationType, mutation *ViewMutation) (string, error) {
	if mutation == nil || !validIdentifier(strings.TrimSpace(mutation.Name)) {
		return "", fmt.Errorf("view name is required and must be an identifier")
	}
	views, _, err := inspectViewSources(source)
	if err != nil {
		return "", err
	}
	derived, err := derivedViewOccurrences(source)
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
	derivedIndex := -1
	for i := range derived {
		if strings.EqualFold(derived[i].Parameter.Name, mutation.Name) {
			derivedIndex = i
			break
		}
	}
	switch operation {
	case OperationAddView:
		if index >= 0 || derivedIndex >= 0 {
			return "", fmt.Errorf("view %q already exists", mutation.Name)
		}
		if normalizeViewKind(mutation.Kind) == spec.RelationKindDerived {
			return addDerivedView(source, views, mutation)
		}
		return addRelatedView(source, views, mutation)
	case OperationUpdateView:
		if derivedIndex >= 0 {
			return updateDerivedView(source, derived[derivedIndex], mutation)
		}
		if index < 0 {
			return "", fmt.Errorf("view %q was not found", mutation.Name)
		}
		if strings.TrimSpace(mutation.SQL) == "" {
			return "", fmt.Errorf("view %q SQL is required", mutation.Name)
		}
		return dql.ApplyPatch(source, views[index].SourceSpan, strings.TrimSpace(mutation.SQL))
	case OperationRemoveView:
		if derivedIndex >= 0 {
			return dql.ApplyPatch(source, derived[derivedIndex].Span, "")
		}
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
	if operation.Type == OperationUpdatePredicateComposition {
		return validatePredicateCompositionResult(operation.PredicateComposition, structure)
	}
	if operation.Type == OperationBatch {
		for _, child := range operation.Operations {
			if err := validateOperationResult(child, structure); err != nil {
				return err
			}
		}
		return nil
	}
	if operation.Type == OperationSetColumnRole && operation.ColumnRole != nil {
		view := findView(structure.Component.RootView, operation.ColumnRole.View)
		if view == nil {
			return fmt.Errorf("column role view %q did not compile", operation.ColumnRole.View)
		}
		for _, column := range view.Columns {
			if column == nil || !strings.EqualFold(column.Source, operation.ColumnRole.Column) && !strings.EqualFold(column.Name, operation.ColumnRole.Column) {
				continue
			}
			actual := compiledColumnGroupable(column)
			want := strings.EqualFold(operation.ColumnRole.Role, "dimension")
			if actual != want {
				return fmt.Errorf("column %s.%s role did not compile as %s", operation.ColumnRole.View, operation.ColumnRole.Column, operation.ColumnRole.Role)
			}
			return nil
		}
		return fmt.Errorf("column %s.%s was not found after role update", operation.ColumnRole.View, operation.ColumnRole.Column)
	}
	if operation.Type == OperationUpdateRelation && operation.Relation != nil {
		if structure == nil || structure.Component == nil || structure.Component.RootView == nil {
			return fmt.Errorf("updated relation graph is unavailable")
		}
		relation := findRelation(structure.Component.RootView, operation.Relation.Name)
		if relation == nil {
			return fmt.Errorf("updated relation %q did not compile", operation.Relation.Name)
		}
		if !strings.EqualFold(strings.TrimSpace(relation.ParentNamespace), strings.TrimSpace(operation.Relation.Parent)) {
			return fmt.Errorf("updated relation %q resolved parent %q, expected %q", operation.Relation.Name, relation.ParentNamespace, operation.Relation.Parent)
		}
	}
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
	if operation.Type == OperationUpdatePredicateGroup && operation.PredicateGroup != nil && structure != nil {
		operator := strings.ToUpper(strings.TrimSpace(operation.PredicateGroup.Operator))
		seen := map[string]bool{}
		for _, site := range structure.PredicateExpansions {
			if site.Group != operation.PredicateGroup.Group {
				continue
			}
			if !slicesContainsFold(operation.PredicateGroup.Views, site.View) {
				return fmt.Errorf("predicate group %d unexpectedly expands in view %q after update", site.Group, site.View)
			}
			if !strings.EqualFold(site.Operator, operator) {
				return fmt.Errorf("predicate group %d in view %q compiled with operator %q, expected %q", site.Group, site.View, site.Operator, operator)
			}
			seen[strings.ToLower(strings.TrimSpace(site.View))] = true
		}
		for _, view := range operation.PredicateGroup.Views {
			if !seen[strings.ToLower(strings.TrimSpace(view))] {
				return fmt.Errorf("predicate group %d did not compile in view %q", operation.PredicateGroup.Group, view)
			}
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
	wantKind := normalizeViewKind(operation.View.Kind)
	if relation.Kind != wantKind {
		return fmt.Errorf("added view %q compiled as %q, expected %q", operation.View.Name, relation.Kind, wantKind)
	}
	if wantKind == spec.RelationKindDerived {
		return nil
	}
	if !strings.EqualFold(strings.TrimSpace(relation.ParentNamespace), parent) {
		return fmt.Errorf("added view %q resolved parent %q, expected %q", operation.View.Name, relation.ParentNamespace, parent)
	}
	return nil
}

func findView(view *spec.View, name string) *spec.View {
	if view == nil {
		return nil
	}
	if strings.EqualFold(view.Name, name) || strings.EqualFold(view.Namespace, name) {
		return view
	}
	for _, relation := range view.Relations {
		if relation != nil {
			if found := findView(relation.View, name); found != nil {
				return found
			}
		}
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
	if normalizeViewKind(mutation.Kind) != spec.RelationKindSubview {
		return "", fmt.Errorf("related view kind must be %q", spec.RelationKindSubview)
	}
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

func addDerivedView(source string, views []ViewOccurrence, mutation *ViewMutation) (string, error) {
	if len(views) == 0 {
		return "", fmt.Errorf("addView requires an existing root view")
	}
	if strings.TrimSpace(mutation.Parent) != "" && !strings.EqualFold(strings.TrimSpace(mutation.Parent), views[0].Name) {
		return "", fmt.Errorf("derived view %q must attach to root view %q", mutation.Name, views[0].Name)
	}
	line, err := renderDerivedView(mutation, "")
	if err != nil {
		return "", err
	}
	prepared := dql.PrepareSource(source)
	if err = prepared.Err(); err != nil {
		return "", err
	}
	if len(prepared.Statements) == 0 {
		return "", fmt.Errorf("addView requires a SQL statement")
	}
	insert := prepared.TrimPrefix + prepared.Statements[0].SQLStart
	return dql.ApplyPatch(source, dql.SourceSpan{Start: insert, End: insert}, line+"\n")
}

func updateDerivedView(source string, occurrence dql.DeclarationOccurrence, mutation *ViewMutation) (string, error) {
	typeExpr := ""
	if occurrence.Parameter != nil {
		typeExpr = occurrence.Parameter.TypeExpr
	}
	line, err := renderDerivedView(mutation, typeExpr)
	if err != nil {
		return "", err
	}
	return dql.ApplyPatch(source, occurrence.Span, line)
}

func renderDerivedView(mutation *ViewMutation, fallbackType string) (string, error) {
	sql := strings.TrimSpace(mutation.SQL)
	if sql == "" {
		return "", fmt.Errorf("derived view %q SQL is required", mutation.Name)
	}
	if strings.Contains(sql, "*/") {
		return "", fmt.Errorf("derived view %q SQL cannot contain a block-comment terminator", mutation.Name)
	}
	if strings.TrimSpace(mutation.Join) != "" || strings.TrimSpace(mutation.On) != "" {
		return "", fmt.Errorf("derived view %q cannot declare join or relation keys", mutation.Name)
	}
	typeExpr := strings.TrimSpace(mutation.TypeExpr)
	if typeExpr == "" {
		typeExpr = strings.TrimSpace(fallbackType)
	}
	if typeExpr == "" {
		typeExpr = text.DetectCaseFormat(mutation.Name).Format(mutation.Name, text.CaseFormatUpperCamel)
	}
	if strings.ContainsAny(typeExpr, "\r\n<>()") {
		return "", fmt.Errorf("derived view %q typeExpr is invalid", mutation.Name)
	}
	return fmt.Sprintf("#define($_ = $%s<%s>(output/derived) /* %s */)", mutation.Name, typeExpr, sql), nil
}

func derivedViewOccurrences(source string) ([]dql.DeclarationOccurrence, error) {
	declarations, err := dql.DeclarationOccurrences(source)
	if err != nil {
		return nil, err
	}
	result := make([]dql.DeclarationOccurrence, 0)
	for _, occurrence := range declarations {
		parameter := occurrence.Parameter
		if parameter != nil && parameter.IsDerivedOutput() {
			result = append(result, occurrence)
		}
	}
	return result, nil
}

func normalizeViewKind(kind spec.RelationKind) spec.RelationKind {
	switch spec.RelationKind(strings.ToLower(strings.TrimSpace(string(kind)))) {
	case "", spec.RelationKindSubview:
		return spec.RelationKindSubview
	case spec.RelationKindDerived:
		return spec.RelationKindDerived
	default:
		return kind
	}
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
