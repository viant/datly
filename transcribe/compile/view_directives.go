package compile

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
)

type viewDirective struct {
	name   string
	target string
	value  string
	values []string
}

func extractViewDirectives(parsed *query.Select) ([]viewDirective, error) {
	if parsed == nil || len(parsed.List) == 0 {
		return nil, nil
	}
	filtered := make(query.List, 0, len(parsed.List))
	result := make([]viewDirective, 0)
	for _, item := range parsed.List {
		directive, recognized, err := parseViewDirective(item)
		if err != nil {
			return nil, err
		}
		if !recognized {
			filtered = append(filtered, item)
			continue
		}
		result = append(result, directive)
	}
	parsed.List = filtered
	if containsViewDirective(parsed) {
		return nil, &Error{Code: CodeViewDirective, Cause: fmt.Errorf("view directives must be standalone SELECT projection items")}
	}
	if len(result) == 0 {
		return nil, nil
	}
	if len(filtered) == 0 {
		return nil, &Error{Code: CodeViewDirective, Cause: fmt.Errorf("view directives cannot be the entire SELECT projection")}
	}
	return result, nil
}

func parseViewDirective(item *query.Item) (viewDirective, bool, error) {
	if item == nil || item.Expr == nil {
		return viewDirective{}, false, nil
	}
	call, ok := item.Expr.(*expr.Call)
	if !ok || call.X == nil {
		return viewDirective{}, false, nil
	}
	name := normalizeViewDirectiveName(sqlparser.Stringify(call.X))
	switch name {
	case spec.ViewControlOrderBy, spec.ViewControlSetLimit, spec.ViewControlUseConnector,
		spec.ViewControlUseCache, spec.ViewControlCacheWarmup,
		spec.ViewControlAllowNulls, spec.ViewControlGroupable, spec.ViewControlGrouping,
		spec.ViewControlAllowedOrder, spec.ViewControlCardinality, spec.ViewControlSelfRef,
		spec.ViewControlType, spec.ViewControlDest, spec.ViewControlBatchSize, spec.ViewControlBatchConcurrency,
		spec.ViewControlMatch, spec.ViewControlPartitioner, spec.ViewControlPublish,
		spec.ViewControlConcurrency, spec.ViewControlEntityHooks:
	default:
		return viewDirective{}, false, nil
	}
	minimum, maximum := 2, 2
	switch name {
	case spec.ViewControlAllowNulls, spec.ViewControlGroupable, spec.ViewControlGrouping, spec.ViewControlPublish:
		minimum, maximum = 1, 1
	case spec.ViewControlSelfRef:
		minimum, maximum = 4, 4
	case spec.ViewControlPartitioner:
		minimum, maximum = 2, 3
	}
	if len(call.Args) < minimum || len(call.Args) > maximum {
		return viewDirective{}, true, &Error{Code: CodeViewDirective, Cause: fmt.Errorf("%s requires %d through %d arguments", name, minimum, maximum)}
	}
	target, ok := viewDirectiveTarget(call.Args[0])
	if !ok {
		return viewDirective{}, true, &Error{Code: CodeViewDirective, Cause: fmt.Errorf("%s target must be a SQL namespace identifier", name)}
	}
	directive := viewDirective{name: name, target: target}
	for index, argument := range call.Args[1:] {
		value, valid := viewDirectiveValue(name, index+1, argument)
		if !valid {
			return viewDirective{}, true, &Error{Code: CodeViewDirective, Cause: fmt.Errorf("%s values must be literals", name)}
		}
		directive.values = append(directive.values, value)
	}
	if len(directive.values) > 0 {
		directive.value = directive.values[0]
	}
	if target == "" || minimum > 1 && directive.value == "" {
		return viewDirective{}, true, &Error{Code: CodeViewDirective, Cause: fmt.Errorf("%s requires non-empty arguments", name)}
	}
	if name == spec.ViewControlSetLimit {
		limit, err := strconv.Atoi(directive.value)
		if err != nil || limit < 0 {
			return viewDirective{}, true, &Error{Code: CodeViewDirective, Cause: fmt.Errorf("set_limit value %q must be a non-negative integer", directive.value)}
		}
	}
	if name == spec.ViewControlBatchSize || name == spec.ViewControlConcurrency || name == spec.ViewControlBatchConcurrency {
		value, err := strconv.Atoi(directive.value)
		if err != nil || value < 0 {
			return viewDirective{}, true, &Error{Code: CodeViewDirective, Cause: fmt.Errorf("%s value %q must be a non-negative integer", name, directive.value)}
		}
	}
	if name == spec.ViewControlPartitioner && len(directive.values) == 2 {
		value, err := strconv.Atoi(directive.values[1])
		if err != nil || value < 0 {
			return viewDirective{}, true, &Error{Code: CodeViewDirective, Cause: fmt.Errorf("set_partitioner concurrency %q must be a non-negative integer", directive.values[1])}
		}
	}
	if name == spec.ViewControlMatch {
		switch strings.ToLower(directive.value) {
		case string(spec.MatchReadAll), string(spec.MatchReadMatched), string(spec.MatchReadDerived):
			directive.value = strings.ToLower(directive.value)
		default:
			return viewDirective{}, true, &Error{Code: CodeViewDirective, Cause: fmt.Errorf("match_strategy value %q is unsupported", directive.value)}
		}
	}
	if name == spec.ViewControlCardinality {
		switch strings.ToLower(directive.value) {
		case "one":
			directive.value = string(spec.CardinalityOne)
		case "many":
			directive.value = string(spec.CardinalityMany)
		default:
			return viewDirective{}, true, &Error{Code: CodeViewDirective, Cause: fmt.Errorf("cardinality value %q must be One or Many", directive.value)}
		}
	}
	return directive, true, nil
}

func containsViewDirective(source node.Node) bool {
	switch actual := source.(type) {
	case *query.Select:
		if containsViewDirective(actual.List) || containsViewDirective(&actual.From) {
			return true
		}
		for _, with := range actual.WithSelects {
			if with != nil && containsViewDirective(with.X) {
				return true
			}
		}
		for _, join := range actual.Joins {
			if containsViewDirective(join) {
				return true
			}
		}
		if actual.Qualify != nil && containsViewDirective(actual.Qualify) {
			return true
		}
		for _, item := range actual.GroupBy {
			if containsViewDirective(item) {
				return true
			}
		}
		if actual.Having != nil && containsViewDirective(actual.Having) {
			return true
		}
		for _, item := range actual.OrderBy {
			if containsViewDirective(item) {
				return true
			}
		}
		return actual.Union != nil && containsViewDirective(actual.Union.X)
	case query.List:
		for _, item := range actual {
			if containsViewDirective(item) {
				return true
			}
		}
	case []node.Node:
		for _, item := range actual {
			if containsViewDirective(item) {
				return true
			}
		}
	case *query.Item:
		return containsViewDirective(actual.Expr)
	case *query.From:
		return containsViewDirective(actual.X)
	case *query.Join:
		return containsViewDirective(actual.With) || containsViewDirective(actual.On)
	case *expr.Qualify:
		return containsViewDirective(actual.X)
	case *expr.Call:
		switch normalizeViewDirectiveName(sqlparser.Stringify(actual.X)) {
		case spec.ViewControlOrderBy, spec.ViewControlSetLimit, spec.ViewControlUseConnector,
			spec.ViewControlUseCache, spec.ViewControlCacheWarmup,
			spec.ViewControlAllowNulls, spec.ViewControlGroupable, spec.ViewControlGrouping,
			spec.ViewControlAllowedOrder, spec.ViewControlCardinality, spec.ViewControlSelfRef,
			spec.ViewControlType, spec.ViewControlDest, spec.ViewControlBatchSize, spec.ViewControlBatchConcurrency,
			spec.ViewControlMatch, spec.ViewControlPartitioner, spec.ViewControlPublish,
			spec.ViewControlConcurrency, spec.ViewControlEntityHooks:
			return true
		}
		if containsViewDirective(actual.X) {
			return true
		}
		for _, argument := range actual.Args {
			if containsViewDirective(argument) {
				return true
			}
		}
	case *expr.Binary:
		return containsViewDirective(actual.X) || containsViewDirective(actual.Y)
	case *expr.Unary:
		return containsViewDirective(actual.X)
	case *expr.Parenthesis:
		return containsViewDirective(actual.X)
	case *expr.Collate:
		return containsViewDirective(actual.X)
	case *expr.Range:
		return containsViewDirective(actual.Min) || containsViewDirective(actual.Max)
	case *expr.Selector:
		return containsViewDirective(actual.X)
	case *expr.Star:
		return containsViewDirective(actual.X)
	case *expr.Raw:
		return containsViewDirective(actual.X)
	}
	return false
}

func viewDirectiveTarget(source node.Node) (string, bool) {
	identifier, ok := source.(*expr.Ident)
	if !ok {
		return "", false
	}
	value := strings.TrimSpace(identifier.Name)
	return value, value != ""
}

func viewDirectiveValue(name string, argument int, source node.Node) (string, bool) {
	switch actual := source.(type) {
	case *expr.Literal:
		numeric := name == spec.ViewControlSetLimit || name == spec.ViewControlBatchSize || name == spec.ViewControlConcurrency || name == spec.ViewControlBatchConcurrency ||
			name == spec.ViewControlPartitioner && argument == 2
		if numeric && actual.Kind != "int" {
			return "", false
		}
		if !numeric && actual.Kind != "string" {
			return "", false
		}
		value := normalizeViewDirectiveValue(actual.Value)
		return value, value != ""
	case *expr.Ident:
		switch name {
		case spec.ViewControlUseConnector, spec.ViewControlUseCache, spec.ViewControlCacheWarmup:
		default:
			return "", false
		}
		value := strings.TrimSpace(actual.Name)
		return value, value != ""
	default:
		return "", false
	}
}

func applyViewDirectives(root *spec.View, directives []viewDirective) error {
	seen := map[*spec.View]map[string]bool{}
	for _, directive := range directives {
		target := findDirectiveView(root, directive.target, map[*spec.View]bool{})
		if target == nil {
			return &Error{Code: CodeViewDirective, Cause: fmt.Errorf("%s target %q does not match a compiled view", directive.name, directive.target)}
		}
		if key := singletonViewDirectiveKey(directive.name); key != "" {
			if seen[target] == nil {
				seen[target] = map[string]bool{}
			}
			if seen[target][key] {
				return &Error{Code: CodeViewDirective, Cause: fmt.Errorf("%s target %q is declared more than once", directive.name, directive.target)}
			}
			seen[target][key] = true
		}
		if target.Source == nil {
			target.Source = &spec.ViewSource{}
		}
		switch directive.name {
		case spec.ViewControlOrderBy:
			if target.Source.Controls == nil {
				target.Source.Controls = &spec.ViewControls{}
			}
			target.Source.Controls.OrderBy = directive.value
		case spec.ViewControlSetLimit:
			limit, _ := strconv.Atoi(directive.value)
			if limit == 0 {
				if target.Selector == nil {
					target.Selector = &spec.Selector{}
				}
				target.Selector.NoLimit = true
				if target.Source.Controls != nil {
					target.Source.Controls.Limit = nil
				}
				continue
			}
			if target.Source.Controls == nil {
				target.Source.Controls = &spec.ViewControls{}
			}
			if target.Selector != nil {
				target.Selector.NoLimit = false
			}
			target.Source.Controls.Limit = &limit
		case spec.ViewControlUseConnector:
			if target.Source.Bindings == nil {
				target.Source.Bindings = &spec.ViewBindings{}
			}
			target.Source.Bindings.Connector = directive.value
		case spec.ViewControlUseCache:
			if target.Source.Bindings == nil {
				target.Source.Bindings = &spec.ViewBindings{}
			}
			target.Source.Bindings.CacheName = directive.value
		case spec.ViewControlCacheWarmup:
			if target.Source.Bindings == nil {
				target.Source.Bindings = &spec.ViewBindings{}
			}
			target.Source.Bindings.CacheWarmup = directive.value
		case spec.ViewControlAllowNulls:
			value := true
			target.AllowNulls = &value
		case spec.ViewControlGroupable, spec.ViewControlGrouping:
			value := true
			target.Groupable = &value
		case spec.ViewControlAllowedOrder:
			if err := applyAllowedOrderBy(target, directive.value); err != nil {
				return &Error{Code: CodeViewDirective, Cause: err}
			}
		case spec.ViewControlCardinality:
			target.Cardinality = spec.Cardinality(directive.value)
			if relation := findDirectiveRelation(root, target); relation != nil {
				relation.Cardinality = target.Cardinality
			}
		case spec.ViewControlSelfRef:
			target.SelfReference = &spec.SelfReference{
				Holder: directive.values[0], Child: directive.values[1], Parent: directive.values[2],
			}
		case spec.ViewControlType:
			target.TypeName = directive.value
		case spec.ViewControlDest:
			target.Dest = directive.value
		case spec.ViewControlEntityHooks:
			if existing := strings.TrimSpace(target.EntityHooks); existing != "" && existing != directive.value {
				return &Error{Code: CodeViewDirective, Cause: fmt.Errorf("%s target %q conflicts with existing entity hooks reference %q", directive.name, directive.target, existing)}
			}
			target.EntityHooks = directive.value
		case spec.ViewControlBatchSize:
			target.BatchSize, _ = strconv.Atoi(directive.value)
		case spec.ViewControlBatchConcurrency:
			target.BatchConcurrency, _ = strconv.Atoi(directive.value)
		case spec.ViewControlPublish:
			target.PublishParent = true
		case spec.ViewControlConcurrency:
			target.RelationalConcurrency, _ = strconv.Atoi(directive.value)
		case spec.ViewControlPartitioner:
			target.Partitioning = &spec.Partitioning{Type: directive.value}
			if len(directive.values) == 2 {
				target.Partitioning.Concurrency, _ = strconv.Atoi(directive.values[1])
			}
		case spec.ViewControlMatch:
			relation := findDirectiveRelation(root, target)
			if relation == nil {
				return &Error{Code: CodeViewDirective, Cause: fmt.Errorf("match_strategy target %q must be a related view", directive.target)}
			}
			relation.MatchStrategy = spec.MatchStrategy(directive.value)
		}
	}
	return nil
}

func singletonViewDirectiveKey(name string) string {
	switch name {
	case spec.ViewControlAllowedOrder:
		return ""
	case spec.ViewControlGrouping:
		return spec.ViewControlGroupable
	default:
		return name
	}
}

func applyAllowedOrderBy(target *spec.View, value string) error {
	if target.Selector == nil {
		target.Selector = &spec.Selector{}
	}
	target.Selector.AllowOrderBy = true
	if target.Selector.OrderAliases == nil {
		target.Selector.OrderAliases = map[string]spec.FieldPath{}
	}
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			return fmt.Errorf("allowed_order_by_columns contains an empty column")
		}
		alias, column := item, item
		if index := strings.Index(item, ":"); index >= 0 {
			alias = strings.TrimSpace(item[:index])
			column = strings.TrimSpace(item[index+1:])
		}
		if alias == "" || column == "" {
			return fmt.Errorf("allowed_order_by_columns entry %q requires alias and column", item)
		}
		if alias == column {
			if existing, ok := orderAlias(target.Selector.OrderAliases, alias); ok && !strings.EqualFold(string(existing), column) {
				return fmt.Errorf("allowed_order_by_columns alias %q is ambiguous", alias)
			}
			target.Selector.Orderable = appendUniqueFieldPath(target.Selector.Orderable, spec.FieldPath(column))
			continue
		}
		if containsFieldPath(target.Selector.Orderable, alias) {
			return fmt.Errorf("allowed_order_by_columns alias %q is ambiguous", alias)
		}
		if err := setOrderAlias(target.Selector.OrderAliases, alias, spec.FieldPath(column)); err != nil {
			return err
		}
		if index := strings.LastIndex(alias, "."); index >= 0 && index+1 < len(alias) {
			terminal := alias[index+1:]
			if containsFieldPath(target.Selector.Orderable, terminal) {
				return fmt.Errorf("allowed_order_by_columns alias %q is ambiguous", terminal)
			}
			if err := setOrderAlias(target.Selector.OrderAliases, terminal, spec.FieldPath(column)); err != nil {
				return err
			}
		}
	}
	return nil
}

func orderAlias(source map[string]spec.FieldPath, alias string) (spec.FieldPath, bool) {
	for key, value := range source {
		if strings.EqualFold(key, alias) {
			return value, true
		}
	}
	return "", false
}

func containsFieldPath(source []spec.FieldPath, candidate string) bool {
	for _, item := range source {
		if strings.EqualFold(string(item), candidate) {
			return true
		}
	}
	return false
}

func setOrderAlias(target map[string]spec.FieldPath, alias string, column spec.FieldPath) error {
	for existingAlias, existingColumn := range target {
		if !strings.EqualFold(existingAlias, alias) {
			continue
		}
		if strings.EqualFold(string(existingColumn), string(column)) {
			return nil
		}
		return fmt.Errorf("allowed_order_by_columns alias %q is ambiguous", alias)
	}
	target[alias] = column
	return nil
}

func appendUniqueFieldPath(target []spec.FieldPath, value spec.FieldPath) []spec.FieldPath {
	for _, item := range target {
		if strings.EqualFold(string(item), string(value)) {
			return target
		}
	}
	return append(target, value)
}

func findDirectiveRelation(root, target *spec.View) *spec.Relation {
	if root == nil || target == nil {
		return nil
	}
	for _, relation := range root.Relations {
		if relation == nil {
			continue
		}
		if relation.View == target {
			return relation
		}
		if found := findDirectiveRelation(relation.View, target); found != nil {
			return found
		}
	}
	return nil
}

func findDirectiveView(view *spec.View, target string, visited map[*spec.View]bool) *spec.View {
	if view == nil || visited[view] {
		return nil
	}
	visited[view] = true
	// DQL view-control targets are SQL namespaces. Metadata names are not a
	// fallback because they can collide with a different joined namespace.
	if strings.EqualFold(strings.TrimSpace(view.Namespace), target) {
		return view
	}
	for _, relation := range view.Relations {
		if relation == nil {
			continue
		}
		if found := findDirectiveView(relation.View, target, visited); found != nil {
			return found
		}
	}
	return nil
}

func normalizeViewDirectiveName(value string) string {
	return strings.ToLower(strings.Trim(strings.TrimSpace(value), "`\"'"))
}

func normalizeViewDirectiveValue(value string) string {
	value = strings.TrimSpace(value)
	if len(value) >= 2 {
		first, last := value[0], value[len(value)-1]
		if first == last && (first == '\'' || first == '"' || first == '`') {
			return strings.TrimSpace(value[1 : len(value)-1])
		}
	}
	return strings.Trim(value, "`\"")
}
