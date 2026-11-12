package gateway

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/datly/repository"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

// Selector parameters retain their actual HTTP binding. Only their public MCP
// field names change for child relations; a child must never accept a root alias.
func mcpSelectorParameters(root *view.View) []*state.Parameter {
	var result []*state.Parameter
	active := map[*view.View]bool{}
	var visit func(*view.View, string)
	visit = func(v *view.View, prefix string) {
		if v == nil || active[v] {
			return
		}
		active[v] = true
		defer delete(active, v)
		if c := v.Selector; c != nil {
			constraints := c.Constraints
			entries := []struct {
				name                string
				parameter, fallback *state.Parameter
				enabled             bool
			}{
				{"Limit", c.LimitParameter, view.QueryStateParameters.LimitParameter, constraints != nil && constraints.Limit},
				{"Offset", c.OffsetParameter, view.QueryStateParameters.OffsetParameter, constraints != nil && constraints.Offset},
				{"Fields", c.FieldsParameter, view.QueryStateParameters.FieldsParameter, constraints != nil && constraints.Projection},
				{"OrderBy", c.OrderByParameter, view.QueryStateParameters.OrderByParameter, constraints != nil && constraints.OrderBy},
				{"Page", c.PageParameter, view.QueryStateParameters.PageParameter, constraints != nil && constraints.IsPageEnabled()},
				{"Criteria", c.CriteriaParameter, view.QueryStateParameters.CriteriaParameter, constraints != nil && constraints.Criteria},
			}
			for _, entry := range entries {
				p := entry.parameter
				// HTTP's implicit fallback belongs to the root only. Inventing a
				// child binding here would advertise an operation HTTP cannot apply.
				if p == nil && prefix == "" && entry.enabled {
					p = entry.fallback
				}
				if p == nil || p.In == nil || p.In.Kind != state.KindQuery || strings.TrimSpace(p.In.Name) == "" || p.Schema == nil || p.Schema.Type() == nil {
					continue
				}
				copy := *p
				copy.Name = prefix + entry.name
				result = append(result, &copy)
			}
		}
		for _, relation := range v.With {
			if relation == nil || relation.Of == nil {
				continue
			}
			name := relation.Holder
			if name == "" {
				name = relation.Name
			}
			if name == "" {
				continue
			}
			name = strings.ReplaceAll(name, ".", "_")
			visit(&relation.Of.View, prefix+mcpUpperCamelToken(name))
		}
	}
	visit(root, "")
	return result
}

func mcpComponentParameters(component *repository.Component) []*state.Parameter {
	if component == nil {
		return nil
	}
	selectors := mcpSelectorParameters(component.View)
	byBinding := map[string]*state.Parameter{}
	// A shared HTTP binding is one control, not independently pageable children.
	// Preserve the first (root-first) owner instead of relabelling the root.
	for _, p := range selectors {
		if byBinding[p.In.Name] == nil {
			byBinding[p.In.Name] = p
		}
	}
	result := make([]*state.Parameter, 0, len(component.Input.Type.Parameters)+len(selectors))
	seen := map[string]bool{}
	for _, p := range append(append(state.Parameters{}, component.Input.Type.Parameters...), selectors...) {
		if p == nil || p.In == nil {
			continue
		}
		if p.In.Kind == state.KindQuery {
			if selector := byBinding[p.In.Name]; selector != nil {
				p = selector
			}
		}
		key := string(p.In.Kind) + "\x00" + p.In.Name
		if (p.In.Kind == state.KindQuery || p.In.Kind == state.KindForm) && seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, p)
	}
	return result
}

func mcpQueryList(value interface{}) ([]string, bool) {
	rv := reflect.ValueOf(value)
	if !rv.IsValid() || (rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array) {
		return nil, false
	}
	items := make([]string, 0, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		value := rv.Index(i).Interface()
		switch number := value.(type) {
		case float64:
			items = append(items, strconv.FormatFloat(number, 'f', -1, 64))
		case float32:
			items = append(items, strconv.FormatFloat(float64(number), 'f', -1, 32))
		default:
			items = append(items, fmt.Sprint(value))
		}
	}
	return items, true
}
