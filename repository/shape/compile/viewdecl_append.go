package compile

import (
	"reflect"
	"strings"

	"github.com/viant/datly/repository/shape/compile/pipeline"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/sqlparser"
)

func appendDeclaredViews(rawDQL string, result *plan.Result) {
	if result == nil {
		return
	}
	declared, diags := extractDeclaredViews(rawDQL)
	if len(diags) > 0 {
		result.Diagnostics = append(result.Diagnostics, diags...)
	}
	for _, item := range declared {
		if item == nil || strings.TrimSpace(item.Name) == "" || strings.TrimSpace(item.SQL) == "" {
			continue
		}
		if item.VirtualSummary {
			if root := lookupRootView(result); root != nil && strings.TrimSpace(root.Summary) == "" {
				root.Summary = strings.TrimSpace(item.SQL)
				root.SummaryName = strings.TrimSpace(item.Name)
			}
			continue
		}
		if parent := lookupSummaryParentView(result, item.SQL); parent != nil {
			if strings.TrimSpace(parent.Summary) == "" {
				parent.Summary = normalizeSummarySQLForParent(parent, item.SQL)
				parent.SummaryName = strings.TrimSpace(item.Name)
			}
			continue
		}
		if _, exists := result.ViewsByName[item.Name]; exists {
			continue
		}
		view := &plan.View{
			Path:        item.Name,
			Holder:      item.Name,
			Name:        item.Name,
			Table:       item.Name,
			SQL:         item.SQL,
			SQLURI:      item.URI,
			Connector:   item.Connector,
			Cardinality: "many",
			FieldType:   reflect.TypeOf([]map[string]interface{}{}),
			ElementType: reflect.TypeOf(map[string]interface{}{}),
			Declaration: buildViewDeclaration(item),
		}
		if item.Required && !item.CardinalitySet {
			view.Cardinality = "one"
		}
		if item.Cardinality != "" {
			view.Cardinality = item.Cardinality
		}
		if queryNode, err := sqlparser.ParseQuery(item.SQL); err == nil && queryNode != nil {
			if inferredName, inferredTable, err := pipeline.InferRoot(queryNode, item.Name); err == nil {
				_ = inferredName
				if strings.TrimSpace(inferredTable) != "" {
					view.Table = inferredTable
				}
			}
			if fType, eType, card := pipeline.InferProjectionType(queryNode); fType != nil && eType != nil {
				view.FieldType = fType
				view.ElementType = eType
				if item.Cardinality == "" && !(item.Required && !item.CardinalitySet) {
					view.Cardinality = card
				}
			}
		}
		result.Views = append(result.Views, view)
		result.ViewsByName[view.Name] = view
	}
}

func normalizeSummarySQLForParent(parent *plan.View, sqlText string) string {
	normalized := strings.TrimSpace(sqlText)
	if parent == nil || normalized == "" {
		return normalized
	}
	parentName := strings.TrimSpace(parent.Name)
	if parentName == "" {
		return normalized
	}
	for _, candidate := range []string{
		"$View." + parentName + ".SQL",
		"$view." + strings.ToLower(parentName) + ".sql",
	} {
		normalized = strings.ReplaceAll(normalized, candidate, "$View.NonWindowSQL")
	}
	return normalized
}

func lookupRootView(result *plan.Result) *plan.View {
	if result == nil {
		return nil
	}
	if len(result.Views) > 0 && result.Views[0] != nil {
		return result.Views[0]
	}
	for _, item := range result.ViewsByName {
		if item != nil {
			return item
		}
	}
	return nil
}

func lookupSummaryParentView(result *plan.Result, sqlText string) *plan.View {
	if result == nil || strings.TrimSpace(sqlText) == "" {
		return nil
	}
	if hasRootSummaryReference(sqlText) {
		if len(result.Views) > 0 && result.Views[0] != nil {
			return result.Views[0]
		}
	}
	parent, ok := findSummaryParentReference(sqlText)
	if !ok {
		return nil
	}
	if view, ok := result.ViewsByName[parent]; ok && view != nil {
		return view
	}
	sanitized := pipeline.SanitizeName(parent)
	if sanitized != "" {
		if view, ok := result.ViewsByName[sanitized]; ok && view != nil {
			return view
		}
	}
	for name, view := range result.ViewsByName {
		if view == nil {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(name), parent) || (sanitized != "" && strings.EqualFold(strings.TrimSpace(name), sanitized)) {
			return view
		}
	}
	for _, view := range result.Views {
		if view == nil {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(view.Name), parent) || (sanitized != "" && strings.EqualFold(strings.TrimSpace(view.Name), sanitized)) {
			return view
		}
	}
	return nil
}

func findSummaryParentReference(input string) (string, bool) {
	if strings.TrimSpace(input) == "" {
		return "", false
	}
	lower := strings.ToLower(input)
	for i := 0; i+len("$view.") < len(lower); i++ {
		if lower[i] != '$' {
			continue
		}
		if !strings.HasPrefix(lower[i:], "$view.") {
			continue
		}
		start := i + len("$view.")
		if start >= len(input) || !isCompileIdentifierStart(input[start]) {
			continue
		}
		end := start + 1
		for end < len(input) && isCompileIdentifierPart(input[end]) {
			end++
		}
		if !strings.HasPrefix(lower[end:], ".sql") {
			continue
		}
		parent := strings.TrimSpace(input[start:end])
		if parent == "" {
			continue
		}
		return parent, true
	}
	return "", false
}

func hasRootSummaryReference(input string) bool {
	if strings.TrimSpace(input) == "" {
		return false
	}
	return strings.Contains(strings.ToLower(input), "$view.nonwindowsql")
}

func isCompileIdentifierStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isCompileIdentifierPart(ch byte) bool {
	return isCompileIdentifierStart(ch) || (ch >= '0' && ch <= '9')
}

func buildViewDeclaration(item *declaredView) *plan.ViewDeclaration {
	if item == nil {
		return nil
	}
	ret := &plan.ViewDeclaration{
		Tag:           item.Tag,
		TypeName:      item.TypeName,
		Dest:          item.Dest,
		Codec:         item.Codec,
		CodecArgs:     append([]string{}, item.CodecArgs...),
		HandlerName:   item.HandlerName,
		HandlerArgs:   append([]string{}, item.HandlerArgs...),
		StatusCode:    item.StatusCode,
		ErrorMessage:  item.ErrorMessage,
		QuerySelector: item.QuerySelector,
		CacheRef:      item.CacheRef,
		Limit:         item.Limit,
		Cacheable:     item.Cacheable,
		When:          item.When,
		Scope:         item.Scope,
		DataType:      item.DataType,
		Of:            item.Of,
		Value:         item.Value,
		Async:         item.Async,
		Output:        item.Output,
	}
	if len(item.Predicates) > 0 {
		ret.Predicates = make([]*plan.ViewPredicate, 0, len(item.Predicates))
		for _, predicate := range item.Predicates {
			ret.Predicates = append(ret.Predicates, &plan.ViewPredicate{
				Name:      predicate.Name,
				Source:    predicate.Source,
				Ensure:    predicate.Ensure,
				Arguments: append([]string{}, predicate.Arguments...),
			})
		}
	}
	if len(item.ColumnsConfig) > 0 {
		ret.ColumnsConfig = map[string]*plan.ViewColumnConfig{}
		for name, cfg := range item.ColumnsConfig {
			if strings.TrimSpace(name) == "" || cfg == nil {
				continue
			}
			ret.ColumnsConfig[name] = &plan.ViewColumnConfig{
				DataType: strings.TrimSpace(cfg.DataType),
				Tag:      strings.TrimSpace(cfg.Tag),
			}
		}
		if len(ret.ColumnsConfig) == 0 {
			ret.ColumnsConfig = nil
		}
	}
	if ret.Tag == "" && ret.TypeName == "" && ret.Dest == "" &&
		ret.Codec == "" && len(ret.CodecArgs) == 0 && ret.HandlerName == "" &&
		len(ret.HandlerArgs) == 0 && ret.StatusCode == nil && ret.ErrorMessage == "" &&
		ret.QuerySelector == "" && ret.CacheRef == "" && ret.Limit == nil && ret.Cacheable == nil &&
		ret.When == "" && ret.Scope == "" && ret.DataType == "" && ret.Of == "" && ret.Value == "" &&
		!ret.Async && !ret.Output && len(ret.Predicates) == 0 && len(ret.ColumnsConfig) == 0 {
		return nil
	}
	return ret
}
