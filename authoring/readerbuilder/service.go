package readerbuilder

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	predicatevelty "github.com/viant/datly/runtime/predicate/velty"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/typecatalog"
	sqltext "github.com/viant/sqlparser/source"
)

type Config struct {
	Scope               string
	Name                string
	Types               *typecatalog.Catalog
	AvailableConnectors []string
	AvailableCaches     []string
	PredicateNames      []string
}

type Service struct{ config Config }

func New(config Config) *Service {
	config.AvailableConnectors = append([]string(nil), config.AvailableConnectors...)
	sort.Strings(config.AvailableConnectors)
	config.AvailableCaches = append([]string(nil), config.AvailableCaches...)
	sort.Strings(config.AvailableCaches)
	config.PredicateNames = append(predicatevelty.Names(), config.PredicateNames...)
	sort.Strings(config.PredicateNames)
	return &Service{config: config}
}

func (s *Service) Apply(ctx context.Context, request Request) Response {
	response := Response{DQL: request.DQL}
	if request.Operation.Type == OperationInspect {
		if err := request.Operation.validate(); err != nil {
			response.Structure, response.Diagnostics = s.inspect(ctx, request.DQL)
			response.Diagnostics = append(response.Diagnostics, operationDiagnostic(err))
			return response
		}
		response.Applied = true
		response.Structure, response.Diagnostics = s.inspect(ctx, request.DQL)
		return response
	}
	candidate, err := s.edit(request.DQL, request.Operation)
	if err != nil {
		response.Structure, response.Diagnostics = s.inspect(ctx, request.DQL)
		response.Diagnostics = append(response.Diagnostics, operationDiagnostic(err))
		return response
	}
	structure, diagnostics := s.inspect(ctx, candidate)
	if len(diagnostics) > 0 {
		response.Structure, response.Diagnostics = s.inspect(ctx, request.DQL)
		response.Diagnostics = append(response.Diagnostics, operationDiagnostic(fmt.Errorf("candidate DQL did not compile")))
		response.Diagnostics = append(response.Diagnostics, diagnostics...)
		return response
	}
	if err = validateOperationResult(request.Operation, structure); err != nil {
		response.Structure, response.Diagnostics = s.inspect(ctx, request.DQL)
		response.Diagnostics = append(response.Diagnostics, operationDiagnostic(err))
		return response
	}
	response.Applied = true
	response.DQL = candidate
	response.Structure = structure
	return response
}

func (s *Service) inspect(ctx context.Context, source string) (*Structure, []*transcribe.Diagnostic) {
	declarations, declarationErr := dql.DeclarationOccurrences(source)
	views, expansions, authoringErr := inspectViewSources(source)
	functions, functionErr := inspectFunctions(source)
	compositions := inspectPredicateCompositions(source, views)
	structure := &Structure{Status: "complete", Declarations: declarations, Views: views,
		PredicateExpansions: expansions, Functions: functions, ColumnContracts: inspectColumnContracts(functions), AvailableConnectors: append([]string(nil), s.config.AvailableConnectors...),
		AvailablePredicates: append([]string(nil), s.config.PredicateNames...)}
	structure.AvailableCaches = append([]string(nil), s.config.AvailableCaches...)
	structure.PredicateCompositions = compositions
	name := strings.TrimSpace(s.config.Name)
	if name == "" {
		name = "Reader"
	}
	result, err := transcribe.NewCompiler().Compile(ctx, &transcribe.Source{
		Scope: strings.TrimSpace(s.config.Scope), Name: name, Text: source, Types: s.config.Types,
	})
	if err == nil {
		structure.Component = result.Component.Clone()
	} else {
		structure.Status = "partial"
	}
	diagnostics := compileDiagnostics(err)
	if declarationErr != nil {
		structure.Status = "partial"
		diagnostics = append(diagnostics, operationDiagnostic(declarationErr))
	}
	if authoringErr != nil {
		structure.Status = "partial"
		diagnostics = append(diagnostics, operationDiagnostic(authoringErr))
	}
	if functionErr != nil {
		structure.Status = "partial"
		diagnostics = append(diagnostics, operationDiagnostic(functionErr))
	}
	return structure, diagnostics
}

func compileDiagnostics(err error) []*transcribe.Diagnostic {
	if err == nil {
		return nil
	}
	if actual, ok := err.(*transcribe.CompileError); ok && len(actual.Diagnostics) > 0 {
		return actual.Diagnostics
	}
	return []*transcribe.Diagnostic{operationDiagnostic(err)}
}

func operationDiagnostic(err error) *transcribe.Diagnostic {
	return &transcribe.Diagnostic{Code: "DQL-BUILDER", Severity: transcribe.SeverityError, Message: err.Error(),
		Span: transcribe.Span{Start: transcribe.Position{Line: 1, Char: 1}, End: transcribe.Position{Line: 1, Char: 1}}}
}

func (s *Service) edit(source string, operation Operation) (string, error) {
	if err := operation.validate(); err != nil {
		return "", err
	}
	switch operation.Type {
	case OperationInspect:
		return source, nil
	case OperationCreateReader:
		return s.createReader(source, operation.Reader)
	case OperationSetPackage:
		return dql.SetPackage(source, operation.Package.Path, operation.Package.Expected)
	case OperationAddField:
		return addField(source, operation.Field)
	case OperationUpdateField, OperationRemoveField:
		return editField(source, operation.Type, operation.Field)
	case OperationAddFieldPredicate, OperationUpdateFieldPredicate, OperationRemoveFieldPredicate:
		return s.editPredicate(source, operation.Type, operation.Predicate)
	case OperationUpdatePredicateGroup:
		return updatePredicateGroup(source, operation.PredicateGroup)
	case OperationUpdatePredicateComposition:
		return updatePredicateComposition(source, operation.PredicateComposition)
	case OperationAddFunction, OperationUpdateFunction, OperationRemoveFunction:
		return s.editFunction(source, operation.Type, operation.Function)
	case OperationSetSetting:
		return s.editSetting(source, operation.Setting)
	case OperationAddView, OperationUpdateView, OperationRemoveView:
		return editView(source, operation.Type, operation.View)
	case OperationUpdateRelation:
		return updateRelation(source, operation.Relation)
	case OperationSetColumnRole:
		return s.setColumnRole(source, operation.ColumnRole)
	case OperationSetColumnContract:
		return s.setColumnContract(source, operation.Column)
	case OperationBatch:
		candidate := source
		for _, child := range operation.Operations {
			var err error
			candidate, err = s.edit(candidate, child)
			if err != nil {
				return "", err
			}
		}
		return candidate, nil
	default:
		return "", fmt.Errorf("unsupported reader builder operation %q", operation.Type)
	}
}

func (o Operation) validate() error {
	payloads := 0
	for _, present := range []bool{o.Reader != nil, o.Package != nil, o.Field != nil, o.Predicate != nil, o.PredicateGroup != nil, o.PredicateComposition != nil, o.Function != nil, o.Setting != nil, o.View != nil, o.Relation != nil, o.ColumnRole != nil, o.Column != nil, len(o.Operations) > 0} {
		if present {
			payloads++
		}
	}
	want := 1
	if o.Type == OperationInspect {
		want = 0
	}
	if payloads != want {
		return fmt.Errorf("operation %q requires exactly %d payloads, got %d", o.Type, want, payloads)
	}
	switch o.Type {
	case OperationInspect:
	case OperationCreateReader:
		if o.Reader == nil {
			return fmt.Errorf("operation %q requires reader", o.Type)
		}
	case OperationSetPackage:
		if o.Package == nil || strings.TrimSpace(o.Package.Path) == "" {
			return fmt.Errorf("operation %q requires package path", o.Type)
		}
	case OperationAddField, OperationUpdateField, OperationRemoveField:
		if o.Field == nil {
			return fmt.Errorf("operation %q requires field", o.Type)
		}
	case OperationAddFieldPredicate, OperationUpdateFieldPredicate, OperationRemoveFieldPredicate:
		if o.Predicate == nil {
			return fmt.Errorf("operation %q requires predicate", o.Type)
		}
	case OperationUpdatePredicateGroup:
		if o.PredicateGroup == nil {
			return fmt.Errorf("operation %q requires predicateGroup", o.Type)
		}
	case OperationUpdatePredicateComposition:
		if o.PredicateComposition == nil {
			return fmt.Errorf("operation %q requires predicateComposition", o.Type)
		}
	case OperationAddFunction, OperationUpdateFunction, OperationRemoveFunction:
		if o.Function == nil {
			return fmt.Errorf("operation %q requires function", o.Type)
		}
	case OperationSetSetting:
		if o.Setting == nil {
			return fmt.Errorf("operation %q requires setting", o.Type)
		}
		if o.Setting.Remove && (o.Setting.Args != nil || len(o.Setting.Options) > 0) {
			return fmt.Errorf("remove setting cannot include arguments or options")
		}
		for _, option := range o.Setting.Options {
			if option.ExpectedArgs != nil || option.Occurrence != 0 {
				return fmt.Errorf("setting options use only name and args")
			}
		}
	case OperationAddView, OperationUpdateView, OperationRemoveView:
		if o.View == nil {
			return fmt.Errorf("operation %q requires view", o.Type)
		}
	case OperationUpdateRelation:
		if o.Relation == nil {
			return fmt.Errorf("operation %q requires relation", o.Type)
		}
	case OperationSetColumnRole:
		if o.ColumnRole == nil {
			return fmt.Errorf("operation %q requires columnRole", o.Type)
		}
	case OperationSetColumnContract:
		if o.Column == nil {
			return fmt.Errorf("operation %q requires column", o.Type)
		}
	case OperationBatch:
		if len(o.Operations) == 0 {
			return fmt.Errorf("operation %q requires at least one child operation", o.Type)
		}
		for index := range o.Operations {
			child := o.Operations[index]
			if child.Type == OperationBatch || child.Type == OperationInspect {
				return fmt.Errorf("batch child %d uses unsupported operation %q", index, child.Type)
			}
			if err := child.validate(); err != nil {
				return fmt.Errorf("batch child %d: %w", index, err)
			}
		}
	default:
		return fmt.Errorf("unsupported reader builder operation %q", o.Type)
	}
	return nil
}

func addField(source string, field *Field) (string, error) {
	if field == nil || !validIdentifier(strings.TrimSpace(field.Name)) || strings.TrimSpace(field.Type) == "" || !validIdentifier(strings.TrimSpace(field.SourceKind)) {
		return "", fmt.Errorf("field name, type and sourceKind are required")
	}
	if strings.ContainsAny(field.Type+field.SourceName, "\r\n)") {
		return "", fmt.Errorf("field type and sourceName must be single declaration values")
	}
	declarations, err := dql.DeclarationOccurrences(source)
	if err != nil {
		return "", err
	}
	for _, declaration := range declarations {
		if declaration.Parameter != nil && strings.EqualFold(declaration.Parameter.Name, field.Name) {
			return "", fmt.Errorf("field %q already exists", field.Name)
		}
	}
	line := fmt.Sprintf("#define($_ = $%s<%s>(%s/%s)", field.Name, field.Type, field.SourceKind, field.SourceName)
	if field.Required != nil {
		if *field.Required {
			line += ".Required()"
		} else {
			line += ".Optional()"
		}
	}
	if strings.TrimSpace(field.QuerySelector) != "" {
		line += ".QuerySelector(" + strconv.Quote(strings.TrimSpace(field.QuerySelector)) + ")"
	}
	if field.Value != nil {
		line += ".Value(" + strconv.Quote(*field.Value) + ")"
	}
	if field.URI != nil && strings.TrimSpace(*field.URI) != "" {
		line += ".WithURI(" + strconv.Quote(strings.TrimSpace(*field.URI)) + ")"
	}
	if field.Codec != nil && strings.TrimSpace(field.Codec.Name) != "" {
		line += renderCodecOption(field.Codec)
	}
	if field.EmitOutput != nil && *field.EmitOutput {
		line += ".Output()"
	}
	if field.Description != nil && strings.TrimSpace(*field.Description) != "" {
		line += ".WithDescription(" + strconv.Quote(strings.TrimSpace(*field.Description)) + ")"
	}
	if field.Example != nil {
		line += ".WithExample(" + strconv.Quote(*field.Example) + ")"
	}
	line += ")\n"
	prepared := dql.PrepareSource(source)
	if err := prepared.Err(); err != nil {
		return "", err
	}
	insert := prepared.TrimPrefix
	if insert < 0 || insert > len(source) {
		insert = len(source)
	}
	return dql.ApplyPatch(source, dql.SourceSpan{Start: insert, End: insert}, line)
}

func (s *Service) editPredicate(source string, kind OperationType, mutation *PredicateMutation) (string, error) {
	if mutation == nil || strings.TrimSpace(mutation.Field) == "" {
		return "", fmt.Errorf("predicate field is required")
	}
	if mutation.Group < 0 {
		return "", fmt.Errorf("predicate group must be non-negative")
	}
	if kind != OperationRemoveFieldPredicate && !slicesContainsFold(s.config.PredicateNames, mutation.Name) {
		return "", fmt.Errorf("predicate %q is not available to the reader builder service", mutation.Name)
	}
	declarations, err := dql.DeclarationOccurrences(source)
	if err != nil {
		return "", err
	}
	matching := make([]dql.DeclarationOccurrence, 0)
	for _, declaration := range declarations {
		if declaration.Parameter != nil && strings.EqualFold(declaration.Parameter.Name, mutation.Field) {
			matching = append(matching, declaration)
		}
	}
	if len(matching) != 1 {
		return "", fmt.Errorf("field %q resolves to %d declarations; exactly one is required", mutation.Field, len(matching))
	}
	declaration := matching[0]
	if kind == OperationRemoveFieldPredicate || kind == OperationUpdateFieldPredicate {
		if mutation.Occurrence < 0 || mutation.Occurrence >= len(declaration.Predicates) {
			return "", fmt.Errorf("predicate occurrence %d is out of range for field %q", mutation.Occurrence, mutation.Field)
		}
		span := declaration.Predicates[mutation.Occurrence].Span
		replacement := ""
		if kind == OperationUpdateFieldPredicate {
			replacement, err = dql.RenderPredicateOption(mutation.predicate())
			if err != nil {
				return "", err
			}
			return applyPredicateAndBuilder(source, span, replacement, mutation)
		}
		return dql.ApplyPatch(source, span, replacement)
	}
	option, err := dql.RenderPredicateOption(mutation.predicate())
	if err != nil {
		return "", err
	}
	insert := declaration.OptionInsert
	if insert < declaration.Span.Start {
		return "", fmt.Errorf("field %q has an invalid declaration span", mutation.Field)
	}
	return applyPredicateAndBuilder(source, dql.SourceSpan{Start: insert, End: insert}, option, mutation)
}

func (m *PredicateMutation) predicate() *spec.Predicate {
	return &spec.Predicate{Group: m.Group, Name: m.Name, Args: append([]string(nil), m.Args...), ApplyWhenAbsent: m.ApplyWhenAbsent}
}

func updatePredicateGroup(source string, mutation *PredicateGroupMutation) (string, error) {
	if mutation == nil || mutation.Group < 0 {
		return "", fmt.Errorf("predicate group and non-negative group number are required")
	}
	operator := strings.ToUpper(strings.TrimSpace(mutation.Operator))
	if operator != "AND" && operator != "OR" {
		return "", fmt.Errorf("predicate group operator must be AND or OR")
	}
	if len(mutation.Views) == 0 {
		return "", fmt.Errorf("predicate group views are required")
	}
	views, sites, err := inspectViewSources(source)
	if err != nil {
		return "", err
	}
	for _, name := range mutation.Views {
		if !containsView(views, name) {
			return "", fmt.Errorf("predicate group view %q was not found", name)
		}
	}
	var matching []PredicateExpansion
	for _, site := range sites {
		if site.Group != mutation.Group {
			continue
		}
		if !slicesContainsFold(mutation.Views, site.View) {
			return "", fmt.Errorf("predicate group %d also expands in view %q; complete shared scope is required", mutation.Group, site.View)
		}
		matching = append(matching, site)
	}
	if len(matching) == 0 {
		return "", fmt.Errorf("predicate group %d has no compiled expansion sites", mutation.Group)
	}
	for _, name := range mutation.Views {
		found := false
		for _, site := range matching {
			found = found || strings.EqualFold(site.View, name)
		}
		if !found {
			return "", fmt.Errorf("predicate group %d does not expand in declared view %q", mutation.Group, name)
		}
	}
	patches := make([]sourcePatch, 0, len(matching))
	for _, site := range matching {
		original := source[site.SourceSpan.Start:site.SourceSpan.End]
		prefix := "$predicate."
		if strings.HasPrefix(original, "${predicate.") {
			prefix = "${predicate."
		}
		method := site.Method
		if method == "Expand" {
			method = "ExpandWith"
		}
		replacement := fmt.Sprintf(`%s%s(%d, %q)`, prefix, method, mutation.Group, operator)
		patches = append(patches, sourcePatch{span: site.SourceSpan, text: replacement})
	}
	return applySourcePatches(source, patches)
}

func applyPredicateAndBuilder(source string, predicateSpan dql.SourceSpan, predicateText string, mutation *PredicateMutation) (string, error) {
	views, sites, err := inspectViewSources(source)
	if err != nil {
		return "", err
	}
	view, missing, err := validateGroupScope(views, sites, mutation.View, mutation.ExpansionViews, mutation.Group)
	if err != nil {
		return "", err
	}
	patches := []sourcePatch{{span: predicateSpan, text: predicateText}}
	if missing {
		builderPatch, err := planMissingGroup(source, view, sites, mutation)
		if err != nil {
			return "", err
		}
		patches = append(patches, builderPatch)
	}
	return applySourcePatches(source, patches)
}

func planMissingGroup(source string, view ViewOccurrence, sites []PredicateExpansion, mutation *PredicateMutation) (sourcePatch, error) {
	groupOperator := strings.ToUpper(strings.TrimSpace(mutation.GroupOperator))
	if groupOperator == "" {
		groupOperator = "AND"
	}
	if groupOperator != "AND" && groupOperator != "OR" {
		return sourcePatch{}, fmt.Errorf("groupOperator must be AND or OR")
	}
	var targetSites []PredicateExpansion
	for _, site := range sites {
		if strings.EqualFold(site.View, view.Name) {
			if site.Method != "FilterGroup" {
				return sourcePatch{}, fmt.Errorf("view %q uses raw predicate %s; existing builder composition is unresolved", view.Name, site.Method)
			}
			targetSites = append(targetSites, site)
		}
	}
	if len(targetSites) == 0 {
		viewSource := source[view.SourceSpan.Start:view.SourceSpan.End]
		boundary := sqltext.CriteriaBoundary(viewSource)
		prefix := "WHERE"
		if sqltext.HasTopLevelClause(viewSource[:boundary], "where") {
			prefix = "AND"
		}
		builder := fmt.Sprintf("\n${predicate.Builder().CombineAnd($predicate.FilterGroup(%d, \"%s\")).Build(\"%s\")}\n", mutation.Group, groupOperator, prefix)
		at := view.SourceSpan.Start + boundary
		return sourcePatch{span: dql.SourceSpan{Start: at, End: at}, text: builder}, nil
	}
	combine := strings.ToUpper(strings.TrimSpace(mutation.CombineOperator))
	if combine != "AND" && combine != "OR" {
		return sourcePatch{}, fmt.Errorf("view %q already has predicate composition; combineOperator AND or OR is required", view.Name)
	}
	buildAt := -1
	viewSource := source[view.SourceSpan.Start:view.SourceSpan.End]
	for _, site := range targetSites {
		relative := site.SourceSpan.End - view.SourceSpan.Start
		next := nextUnprotectedBuild(viewSource, relative)
		if next < 0 {
			return sourcePatch{}, fmt.Errorf("predicate expansion in view %q has no enclosing builder Build call", view.Name)
		}
		candidate := next
		if buildAt >= 0 && buildAt != candidate {
			return sourcePatch{}, fmt.Errorf("view %q has multiple predicate builders; builder selection is required", view.Name)
		}
		buildAt = candidate
	}
	chain := ".And()"
	if combine == "OR" {
		chain = ".Or()"
	}
	chain += fmt.Sprintf(".CombineAnd($predicate.FilterGroup(%d, \"%s\"))", mutation.Group, groupOperator)
	at := view.SourceSpan.Start + buildAt
	return sourcePatch{span: dql.SourceSpan{Start: at, End: at}, text: chain}, nil
}

func nextUnprotectedBuild(source string, start int) int {
	scanner := sqltext.NewCodeScanner(source, start)
	for position, more := scanner.Next(); more; position, more = scanner.Next() {
		if strings.HasPrefix(source[position:], ".Build(") {
			return position
		}
		if position > start && (strings.HasPrefix(source[position:], "$predicate.Builder(") || strings.HasPrefix(source[position:], "${")) {
			return -1
		}
	}
	return -1
}

type sourcePatch struct {
	span dql.SourceSpan
	text string
}

func applySourcePatches(source string, patches []sourcePatch) (string, error) {
	sort.SliceStable(patches, func(i, j int) bool { return patches[i].span.Start > patches[j].span.Start })
	result := source
	for _, patch := range patches {
		var err error
		result, err = dql.ApplyPatch(result, patch.span, patch.text)
		if err != nil {
			return "", err
		}
	}
	return result, nil
}

func validateGroupScope(views []ViewOccurrence, sites []PredicateExpansion, target string, expansionViews []string, group int) (ViewOccurrence, bool, error) {
	target = strings.TrimSpace(target)
	var selected ViewOccurrence
	for _, view := range views {
		if strings.EqualFold(view.Name, target) {
			selected = view
		}
	}
	if selected.Name == "" {
		return ViewOccurrence{}, false, fmt.Errorf("view %q was not found as a wrapped DQL view", target)
	}
	intended := append([]string(nil), expansionViews...)
	if len(intended) == 0 {
		intended = []string{target}
	}
	if !slicesContainsFold(intended, target) {
		return ViewOccurrence{}, false, fmt.Errorf("expansionViews must include target view %q", target)
	}
	for _, name := range intended {
		if !containsView(views, name) {
			return ViewOccurrence{}, false, fmt.Errorf("expansion view %q was not found", name)
		}
	}
	seenTarget := false
	seen := map[string]bool{}
	for _, site := range sites {
		if site.Group != group {
			continue
		}
		if !slicesContainsFold(intended, site.View) {
			return ViewOccurrence{}, false, fmt.Errorf("predicate group %d is also expanded by view %q; shared scope must be explicit", group, site.View)
		}
		if strings.EqualFold(site.View, target) {
			seenTarget = true
		}
		seen[strings.ToLower(strings.TrimSpace(site.View))] = true
	}
	for _, name := range intended {
		if !strings.EqualFold(name, target) && !seen[strings.ToLower(strings.TrimSpace(name))] {
			return ViewOccurrence{}, false, fmt.Errorf("declared shared expansion view %q does not expand predicate group %d", name, group)
		}
	}
	return selected, !seenTarget, nil
}

func slicesContainsFold(values []string, candidate string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), strings.TrimSpace(candidate)) {
			return true
		}
	}
	return false
}

func inspectViewSources(source string) ([]ViewOccurrence, []PredicateExpansion, error) {
	prepared := dql.PrepareSource(source)
	if err := prepared.Err(); err != nil {
		return nil, nil, err
	}
	var views []ViewOccurrence
	masked := prepared.DirectSQL
	projection, err := outerProjection(source)
	if err != nil {
		return nil, nil, err
	}
	scanner := sqltext.NewCodeScanner(masked, projection.end)
	skipUntil := 0
	for pos, more := scanner.Next(); more; pos, more = scanner.Next() {
		if pos < skipUntil {
			continue
		}
		if masked[pos] != '(' {
			continue
		}
		group, end, ok := sqltext.ReadGroupString(masked, pos, '(', ')')
		if !ok || !startsRead(group[1:len(group)-1]) {
			continue
		}
		cursor := end
		for cursor < len(masked) && sqltext.IsWhitespace(masked[cursor]) {
			cursor++
		}
		if strings.HasPrefix(strings.ToLower(masked[cursor:]), "as ") {
			cursor += 3
		}
		start := cursor
		for cursor < len(masked) && (masked[cursor] == '_' || masked[cursor] >= 'a' && masked[cursor] <= 'z' || masked[cursor] >= 'A' && masked[cursor] <= 'Z' || cursor > start && masked[cursor] >= '0' && masked[cursor] <= '9') {
			cursor++
		}
		if cursor == start {
			continue
		}
		views = append(views, ViewOccurrence{Name: masked[start:cursor], SQL: strings.TrimSpace(source[pos+1 : end-1]), SourceSpan: dql.SourceSpan{Start: pos + 1, End: end - 1}})
		skipUntil = end
	}
	seen := map[string]bool{}
	for _, view := range views {
		key := strings.ToLower(view.Name)
		if seen[key] {
			return nil, nil, fmt.Errorf("wrapped view name %q is not unique", view.Name)
		}
		seen[key] = true
	}
	var sites []PredicateExpansion
	for _, view := range views {
		fragment := source[view.SourceSpan.Start:view.SourceSpan.End]
		for _, method := range []string{"FilterGroup", "ExpandWith", "Expand"} {
			for _, needle := range []string{"$predicate." + method, "${predicate." + method} {
				for from := 0; from < len(fragment); {
					index := strings.Index(fragment[from:], needle)
					if index < 0 {
						break
					}
					index += from
					if sqltext.ProtectionAt(fragment, index) != "" {
						from = index + len(needle)
						continue
					}
					afterName := index + len(needle)
					if afterName < len(fragment) && !sqltext.IsWhitespace(fragment[afterName]) && fragment[afterName] != '(' {
						from = afterName
						continue
					}
					open := afterName
					for open < len(fragment) && sqltext.IsWhitespace(fragment[open]) {
						open++
					}
					groupText, end, ok := sqltext.ReadGroupString(fragment, open, '(', ')')
					if !ok {
						return nil, nil, fmt.Errorf("invalid predicate %s call in view %q", method, view.Name)
					}
					args := sqltext.SplitArgs(groupText[1 : len(groupText)-1])
					group, valid := staticInt(args)
					if !valid {
						return nil, nil, fmt.Errorf("predicate %s group in view %q must be a static integer", method, view.Name)
					}
					operator := ""
					if len(args) > 1 {
						operator = sqltext.TrimQuote(args[1])
					}
					sites = append(sites, PredicateExpansion{View: view.Name, Method: method, Group: group, Operator: operator,
						SourceSpan: dql.SourceSpan{Start: view.SourceSpan.Start + index, End: view.SourceSpan.Start + end}})
					from = end
				}
			}
		}
	}
	return views, sites, nil
}

func startsRead(source string) bool {
	trimmed := strings.TrimSpace(source)
	return strings.HasPrefix(strings.ToLower(trimmed), "select ") || strings.HasPrefix(strings.ToLower(trimmed), "with ")
}

func staticInt(args []string) (int, bool) {
	if len(args) == 0 {
		return 0, false
	}
	value, err := strconv.Atoi(strings.TrimSpace(sqltext.TrimQuote(args[0])))
	return value, err == nil
}
