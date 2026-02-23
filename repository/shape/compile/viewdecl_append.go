package compile

import (
	"reflect"
	"regexp"
	"strings"

	"github.com/viant/datly/repository/shape/compile/pipeline"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/sqlparser"
)

var summaryParentRefExpr = regexp.MustCompile(`(?i)\$View\.([a-zA-Z_][a-zA-Z0-9_]*)\.SQL\b`)

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
		if parent := lookupSummaryParentView(result, item.SQL); parent != nil {
			if strings.TrimSpace(parent.Summary) == "" {
				parent.Summary = strings.TrimSpace(item.SQL)
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
		if item.Cardinality != "" {
			view.Cardinality = item.Cardinality
		}
		if queryNode, err := sqlparser.ParseQuery(item.SQL); err == nil && queryNode != nil {
			if inferredName, inferredTable, err := pipeline.InferRoot(queryNode, item.Name); err == nil {
				view.Name = inferredName
				view.Holder = inferredName
				view.Path = inferredName
				view.Table = inferredTable
			}
			if fType, eType, card := pipeline.InferProjectionType(queryNode); fType != nil && eType != nil {
				view.FieldType = fType
				view.ElementType = eType
				if item.Cardinality == "" {
					view.Cardinality = card
				}
			}
		}
		result.Views = append(result.Views, view)
		result.ViewsByName[view.Name] = view
	}
}

func lookupSummaryParentView(result *plan.Result, sqlText string) *plan.View {
	if result == nil || strings.TrimSpace(sqlText) == "" {
		return nil
	}
	matches := summaryParentRefExpr.FindStringSubmatch(sqlText)
	if len(matches) < 2 {
		return nil
	}
	parent := strings.TrimSpace(matches[1])
	if parent == "" {
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

func buildViewDeclaration(item *declaredView) *plan.ViewDeclaration {
	if item == nil {
		return nil
	}
	ret := &plan.ViewDeclaration{
		Tag:           item.Tag,
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
	if ret.Tag == "" && ret.Codec == "" && len(ret.CodecArgs) == 0 && ret.HandlerName == "" &&
		len(ret.HandlerArgs) == 0 && ret.StatusCode == nil && ret.ErrorMessage == "" &&
		ret.QuerySelector == "" && ret.CacheRef == "" && ret.Limit == nil && ret.Cacheable == nil &&
		ret.When == "" && ret.Scope == "" && ret.DataType == "" && ret.Of == "" && ret.Value == "" &&
		!ret.Async && !ret.Output && len(ret.Predicates) == 0 {
		return nil
	}
	return ret
}
