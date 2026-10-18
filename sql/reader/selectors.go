package reader

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	xhandler "github.com/viant/xdatly/handler"
	xresponse "github.com/viant/xdatly/response"
	xstate "github.com/viant/xdatly/state"
)

type invocationSelectors map[*data.View]*xstate.Selector

func resolveInvocationSelectors(ctx context.Context, session *Session, input reflect.Value, binder xhandler.Binder) (invocationSelectors, error) {
	result := invocationSelectors{}
	var companion reflect.Value
	if plan := session.Artifact.selectorCompanion; plan != nil {
		var err error
		companion, err = plan.Value(binder, input.Interface())
		if err != nil {
			return nil, err
		}
	}
	for index, binding := range session.Artifact.SelectorBindings {
		source := input
		if session.Artifact.companionSelectors[index] {
			source = companion
			if binding.Property == spec.SelectorPropertyPage && session.Artifact.selectorCompanion.RawPageField(binding.FieldIndex) {
				continue
			}
		}
		field, ok := selectorField(source, binding.FieldIndex)
		if !ok {
			continue
		}
		selector := result[binding.View]
		if selector == nil {
			selector = &xstate.Selector{}
			result[binding.View] = selector
		}
		if session.Artifact.selectorCompanion != nil && session.Artifact.selectorCompanion.DeferredPage() && binding.Property == spec.SelectorPropertyFields {
			// Validate only the final merged Fields; an explicit root selector can
			// replace this query list before its names acquire execution authority.
			selector.Fields = make([]string, field.Len())
			for i := range selector.Fields {
				selector.Fields[i] = field.Index(i).String()
			}
		} else if err := applySelectorBinding(selector, binding, field); err != nil {
			return nil, err
		}
		if session.outputAccessors != nil && session.outputAccessors.selectorOwner != nil &&
			binding.View == session.outputAccessors.selectorOwner.view && binding.Property == spec.SelectorPropertyFields && field.Len() > 0 {
			session.selectorOwned = true
		}
	}
	selectors, err := lookupSelectors(ctx, binder)
	if err != nil {
		return nil, err
	}
	rootOverride := false
	overrides := map[*data.View]bool{}
	for _, named := range selectors {
		if named == nil {
			continue
		}
		view, err := session.Artifact.ViewIndex.Resolve(named.Name)
		if err != nil {
			return nil, fmt.Errorf("query selector: %w", err)
		}
		result[view] = named.Selector.Clone()
		overrides[view] = true
		if session.Artifact.Root != nil && view == session.Artifact.Root.View {
			rootOverride = true
		}
		if session.outputAccessors != nil && session.outputAccessors.selectorOwner != nil && view == session.outputAccessors.selectorOwner.view {
			session.selectorOwned = false
		}
	}
	if len(result) == 0 && (session.Artifact.selectorCompanion == nil || !session.Artifact.selectorCompanion.HasPageSources()) {
		return nil, nil
	}
	for view, selector := range result {
		if session.Artifact.selectorCompanion != nil && session.Artifact.selectorCompanion.DeferredPage() {
			for _, name := range append(append([]string(nil), selector.Fields...), selector.Columns...) {
				if strings.TrimSpace(name) != "" {
					continue
				}
				cause := &dsql.UnknownProjectionColumnError{Column: name, RequestedColumn: name}
				if owned := session.selectorError(view, cause, session.selectorOwned); owned != nil {
					return nil, owned
				}
				return nil, invalidSelectorFieldCause(name, "empty field name", cause)
			}
		}
		selector.Fields = append([]string(nil), selector.Fields...)
		selector.Columns = append([]string(nil), selector.Columns...)
		for _, names := range [][]string{selector.Fields, selector.Columns} {
			for i, name := range names {
				var resolved *data.SelectorField
				for at := range view.SelectorFields {
					field := &view.SelectorFields[at]
					if !(dsql.ProjectionNames{field.PublicName, field.GoName, field.Column}).Matches(name) {
						continue
					}
					if resolved != nil && resolved.GoName != field.GoName {
						failure := invalidSelectorField(name, "ambiguous public field").(*xresponse.Error)
						if owned := session.selectorError(view, failure.Cause, session.selectorOwned); owned != nil {
							return nil, owned
						}
						return nil, failure
					}
					resolved = field
				}

				if resolved != nil && resolved.OutputOnly && view.IsGroupable() {
					return nil, invalidSelectorField(name, "output-only selection is not supported for grouped views")
				}
				if resolved != nil && resolved.Holder {
					names[i] = resolved.GoName
				}
			}
		}
		if (len(selector.Fields) > 0 || len(selector.Columns) > 0) && view.Spec.Selector != nil && !view.Spec.Selector.AllowFields {
			failure := invalidSelectorField(strings.Join(selector.Fields, ","), "projection is not allowed").(*xresponse.Error)
			if owned := session.selectorError(view, failure.Cause, session.selectorOwned); owned != nil {
				return nil, owned
			}
			return nil, failure
		}
	}
	if plan := session.Artifact.selectorCompanion; plan != nil && plan.HasPageSources() && !plan.DeferredPage() {
		for _, parameter := range plan.PageParameters() {
			view, err := session.Artifact.ViewIndex.Resolve(parameter.QuerySelector.View)
			if err != nil {
				return nil, err
			}
			if overrides[view] {
				continue
			}
			selector := result[view]
			if selector == nil {
				selector = &xstate.Selector{}
				result[view] = selector
			}
			// Resolve Page even with an Offset: original Datly applies Page last.
			// A NoLimit policy suppresses the default window only; an explicit
			// positive client/source limit still gives Page execution authority.
			policy := view.Spec.Selector
			limit := selector.Limit
			if limit <= 0 && view.Spec.Source != nil && view.Spec.Source.Controls != nil && view.Spec.Source.Controls.Limit != nil {
				limit = *view.Spec.Source.Controls.Limit
			}
			if policy != nil && policy.NoLimit && limit <= 0 {
				continue
			}
			selector.Page, err = plan.ResolvePageParameter(ctx, binder, input.Interface(), parameter)
			if err != nil {
				return nil, err
			}
		}
	}
	if plan := session.Artifact.selectorCompanion; plan != nil && plan.DeferredPage() {
		root := session.Artifact.Root.View
		finalFields := viewProjection(root, result[root])
		selection, err := session.Artifact.staticProjection.Select(finalFields)
		if err != nil {
			if owned := session.selectorError(root, err, session.selectorOwned); owned != nil {
				return nil, owned
			}
			return nil, invalidSelectorFieldCause("Fields", "unknown field in the declared source projection", err)
		}
		session.staticSelection = selection
		session.staticFields = finalFields
		if !rootOverride {
			page, err := plan.ResolvePage(ctx, binder, input.Interface())
			if err != nil {
				session.selectorFailure = err
				return nil, err
			}
			selector := result[root]
			if selector == nil {
				selector = &xstate.Selector{}
				result[root] = selector
			}
			selector.Page = page
		}
	}
	return result, nil
}

func lookupSelectors(ctx context.Context, binder xhandler.Binder) (xstate.Selectors, error) {
	if binder == nil {
		return nil, nil
	}
	value, ok, err := binder.Lookup(ctx, xhandler.SelectorsKey)
	if err != nil || !ok || value == nil {
		return nil, err
	}
	selectors, ok := value.(xstate.Selectors)
	if !ok {
		return nil, fmt.Errorf("selector capability must be state.Selectors, got %T", value)
	}
	return selectors, nil
}

func (s invocationSelectors) forView(view *data.View) *xstate.Selector {
	if s == nil {
		return nil
	}
	return s[view]
}

func selectorField(input reflect.Value, index []int) (reflect.Value, bool) {
	if input.Kind() == reflect.Ptr {
		if input.IsNil() {
			return reflect.Value{}, false
		}
		input = input.Elem()
	}
	for _, fieldIndex := range index {
		for input.Kind() == reflect.Ptr {
			if input.IsNil() {
				return reflect.Value{}, false
			}
			input = input.Elem()
		}
		if input.Kind() != reflect.Struct || fieldIndex < 0 || fieldIndex >= input.NumField() {
			return reflect.Value{}, false
		}
		input = input.Field(fieldIndex)
	}
	for input.Kind() == reflect.Ptr {
		if input.IsNil() {
			return reflect.Value{}, false
		}
		input = input.Elem()
	}
	return input, input.IsValid()
}

func applySelectorBinding(selector *xstate.Selector, binding SelectorBindingPlan, value reflect.Value) error {
	switch binding.Property {
	case spec.SelectorPropertyFields:
		fields := make([]string, value.Len())
		for i := range fields {
			fields[i] = value.Index(i).String()
			// Original field selectors validate every supplied name. An empty
			// string is a valid list item but cannot name a projected column.
			if strings.TrimSpace(fields[i]) == "" {
				return invalidSelectorFieldCause(fields[i], "empty field name", &dsql.UnknownProjectionColumnError{Column: fields[i], RequestedColumn: fields[i]})
			}
		}
		selector.Fields = fields
	case spec.SelectorPropertyOrderBy:
		if value.Kind() == reflect.Slice {
			parts := make([]string, value.Len())
			for i := range parts {
				parts[i] = value.Index(i).String()
			}
			selector.OrderBy = strings.Join(parts, ",")
		} else {
			selector.OrderBy = value.String()
		}
	case spec.SelectorPropertyCriteria:
		selector.Criteria = value.String()
	case spec.SelectorPropertyOffset:
		selector.Offset = int(value.Int())
	case spec.SelectorPropertyLimit:
		selector.Limit = int(value.Int())
	case spec.SelectorPropertyPage:
		selector.Page = int(value.Int())
	default:
		return fmt.Errorf("unsupported query selector property %q", binding.Property)
	}
	return nil
}

func invalidSelectorField(name, reason string) error {
	return invalidSelectorFieldCause(name, reason, nil)
}
func invalidSelectorFieldCause(name, reason string, cause error) error {
	message := fmt.Sprintf("Fields %q: %s", name, reason)
	if cause == nil {
		cause = fmt.Errorf("%s", message)
	}
	return &xresponse.Error{Code: 400, Payload: xresponse.Status{Status: "error", Message: message}, Cause: cause}
}
