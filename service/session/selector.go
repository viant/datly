package session

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/viant/datly/service/session/criteria"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xdatly/handler/response"
	hstate "github.com/viant/xdatly/handler/state"
)

func normalizeSelectorName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	name = strings.ReplaceAll(name, "_", "")
	name = strings.ReplaceAll(name, "-", "")
	name = strings.ReplaceAll(name, ".", "")
	return name
}

func resolveInjectedQuerySelector(ns *view.NamespaceView, selectors hstate.QuerySelectors) *hstate.NamedQuerySelector {
	if len(selectors) == 0 || ns == nil || ns.View == nil {
		return nil
	}
	if selector := selectors.Find(ns.View.Name); selector != nil {
		return selector
	}
	viewName := normalizeSelectorName(ns.View.Name)
	for _, selector := range selectors {
		if selector == nil {
			continue
		}
		if normalizeSelectorName(selector.Name) == viewName {
			return selector
		}
	}
	for _, namespace := range ns.Namespaces {
		if namespace == "" {
			continue
		}
		if selector := selectors.Find(namespace); selector != nil {
			return selector
		}
		nsName := normalizeSelectorName(namespace)
		for _, selector := range selectors {
			if selector == nil {
				continue
			}
			if normalizeSelectorName(selector.Name) == nsName {
				return selector
			}
		}
	}
	// Backward-compatible fallback: a single unnamed selector applies to root view.
	if ns.Root && len(selectors) == 1 {
		if sel := selectors[0]; sel != nil && strings.TrimSpace(sel.Name) == "" {
			return sel
		}
	}
	return nil
}

func (s *Session) setQuerySelector(ctx context.Context, ns *view.NamespaceView, opts *Options) (err error) {
	selectorParameters := ns.View.Selector
	if selectorParameters == nil {
		return nil
	}

	selector := s.state.Lookup(ns.View)

	var injected *hstate.NamedQuerySelector
	if opts != nil && opts.locatorOpt != nil && opts.locatorOpt.QuerySelectors != nil {
		injected = resolveInjectedQuerySelector(ns, opts.locatorOpt.QuerySelectors)
	}
	if err = s.populateFieldQuerySelector(ctx, ns, opts); err != nil {
		return response.NewParameterError(ns.View.Name, selectorParameterName(selectorParameters.FieldsParameter, view.QueryStateParameters.FieldsParameter), err)
	}
	if err = s.populateLimitQuerySelector(ctx, ns, opts); err != nil {
		return response.NewParameterError(ns.View.Name, selectorParameterName(selectorParameters.LimitParameter, view.QueryStateParameters.LimitParameter), err)
	}
	if err = s.populateOffsetQuerySelector(ctx, ns, opts); err != nil {
		return response.NewParameterError(ns.View.Name, selectorParameterName(selectorParameters.OffsetParameter, view.QueryStateParameters.OffsetParameter), err)
	}
	if err = s.populateOrderByQuerySelector(ctx, ns, opts); err != nil {
		return response.NewParameterError(ns.View.Name, selectorParameterName(selectorParameters.OrderByParameter, view.QueryStateParameters.OrderByParameter), err)
	}
	if err = s.populateCriteriaQuerySelector(ctx, ns, opts); err != nil {
		return response.NewParameterError(ns.View.Name, selectorParameterName(selectorParameters.CriteriaParameter, view.QueryStateParameters.CriteriaParameter), err)
	}
	if err = s.populatePageQuerySelector(ctx, ns, opts); err != nil {
		return response.NewParameterError(ns.View.Name, selectorParameterName(selectorParameters.PageParameter, view.QueryStateParameters.PageParameter), err)
	}

	// Apply injected selector last so it takes precedence over request-derived values,
	// but still validate against view selector constraints.
	if injected != nil {
		selector.QuerySelector = injected.QuerySelector
		if err := s.applyInjectedQuerySelector(ns, selector, injected); err != nil {
			return err
		}
	} else if selector.Page > 0 && selector.Offset == 0 {
		// If selector was pre-set (e.g. from non-query sources) without an explicit page parameter,
		// apply Page semantics to compute Offset/Limit.
		_ = s.setPageQuerySelector(selector.Page, ns)
	}
	if selector.Limit == 0 && selector.Offset != 0 {
		return fmt.Errorf("can't use offset without limit - view: %v", ns.View.Name)
	}
	return nil
}

func selectorParameterName(parameter, fallback *state.Parameter) string {
	if parameter != nil && parameter.Name != "" {
		return parameter.Name
	}
	if fallback != nil && fallback.Name != "" {
		return fallback.Name
	}
	return ""
}

func (s *Session) applyInjectedQuerySelector(ns *view.NamespaceView, selector *view.Statelet, injected *hstate.NamedQuerySelector) error {
	if injected == nil || selector == nil {
		return nil
	}
	if len(injected.Fields) > 0 {
		if err := s.setFieldsQuerySelector(injected.Fields, ns); err != nil {
			return err
		}
	}
	if injected.Limit != 0 {
		if err := s.setLimitQuerySelector(injected.Limit, ns); err != nil {
			return err
		}
	}
	if injected.Offset != 0 {
		if err := s.setOffsetQuerySelector(injected.Offset, ns); err != nil {
			return err
		}
	}
	if injected.OrderBy != "" {
		items := strings.Split(injected.OrderBy, ",")
		if err := s.setOrderByQuerySelector(items, ns); err != nil {
			return err
		}
	}
	if injected.Criteria != "" {
		if err := s.setCriteriaQuerySelector(injected.Criteria, ns); err != nil {
			return err
		}
	}
	if injected.Page != 0 {
		if err := s.setPageQuerySelector(injected.Page, ns); err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) setQuerySettings(ctx context.Context, ns *view.NamespaceView, opts *Options) (err error) {
	selectorParameters := ns.View.Selector
	if selectorParameters == nil {
		return nil
	}
	if err = s.populateSyncFlag(ctx, ns, opts); err != nil {
		return response.NewParameterError(ns.View.Name, selectorParameters.SyncFlagParameter.Name, err)
	}
	if err = s.populateContentFormat(ctx, ns, opts); err != nil {
		return response.NewParameterError(ns.View.Name, selectorParameters.ContentFormatParameter.Name, err)
	}
	return nil
}

func (s *Session) populatePageQuerySelector(ctx context.Context, ns *view.NamespaceView, opts *Options) error {
	selectorParameters := ns.View.Selector
	pageParameters := ns.SelectorParameters(selectorParameters.PageParameter, view.QueryStateParameters.PageParameter)
	value, has, err := s.lookupFirstValue(ctx, pageParameters, opts)
	if has && err == nil {
		err = s.setPageQuerySelector(value, ns)
	}
	return err
}

func (s *Session) populateSyncFlag(ctx context.Context, ns *view.NamespaceView, opts *Options) error {
	selectorParameters := ns.View.Selector
	syncFlagParameter := ns.SelectorParameters(selectorParameters.SyncFlagParameter, view.QueryStateParameters.SyncFlagParameter)
	value, has, err := s.lookupFirstValue(ctx, syncFlagParameter, opts)
	if has && err == nil {
		selector := s.state.Lookup(ns.View)
		if !selector.SyncFlag { //one sync mode if already set, do not override
			err = s.setSyncFlag(value, ns)
		}
	}
	return err
}

func (s *Session) populateContentFormat(ctx context.Context, ns *view.NamespaceView, opts *Options) error {
	selectorParameters := ns.View.Selector
	syncFlagParameter := ns.SelectorParameters(selectorParameters.ContentFormatParameter, view.QueryStateParameters.ContentFormatParameter)
	value, has, err := s.lookupFirstValue(ctx, syncFlagParameter, opts)
	if has && err == nil {
		err = s.setContentFormat(value, ns)
	}
	return err
}

func (s *Session) setPageQuerySelector(value interface{}, ns *view.NamespaceView) error {
	page := value.(int)
	selector := s.state.Lookup(ns.View)
	actualLimit := selector.Limit
	if actualLimit == 0 {
		actualLimit = ns.View.Selector.Limit
	}
	selector.Offset = actualLimit * (page - 1)
	selector.Limit = actualLimit
	selector.Page = page
	return nil
}

func (s *Session) setSyncFlag(value interface{}, ns *view.NamespaceView) error {
	flag, _ := value.(bool)
	selector := s.state.Lookup(ns.View)
	selector.SyncFlag = flag
	return nil
}

func (s *Session) setContentFormat(value interface{}, ns *view.NamespaceView) error {
	contentFormat, _ := value.(string)
	settings := s.state.QuerySettings(ns.View)
	if contentFormat != "" {
		settings.ContentFormat = contentFormat
	}
	return nil
}

func (s *Session) populateCriteriaQuerySelector(ctx context.Context, ns *view.NamespaceView, opts *Options) error {
	selectorParameters := ns.View.Selector
	criteriaParameters := ns.SelectorParameters(selectorParameters.CriteriaParameter, view.QueryStateParameters.CriteriaParameter)
	value, has, err := s.lookupFirstValue(ctx, criteriaParameters, opts)
	if has && err == nil {
		err = s.setCriteriaQuerySelector(value, ns)
	}
	return err
}

func (s *Session) populateOrderByQuerySelector(ctx context.Context, ns *view.NamespaceView, opts *Options) error {
	selectorParameters := ns.View.Selector
	orderByParameters := ns.SelectorParameters(selectorParameters.OrderByParameter, view.QueryStateParameters.OrderByParameter)
	value, has, err := s.lookupFirstValue(ctx, orderByParameters, opts)
	if has && err == nil {
		err = s.setOrderByQuerySelector(value, ns)
	}
	return err
}

func (s *Session) setOrderByQuerySelector(value interface{}, ns *view.NamespaceView) error {
	if !ns.View.Selector.Constraints.OrderBy {
		return fmt.Errorf("can't use orderBy on view %v", ns.View.Name)
	}
	items := value.([]string)
	for _, item := range items {
		item = strings.ReplaceAll(item, ":", " ")
		column := item
		if index := strings.Index(item, " "); index != -1 {
			column = item[:index]
		}
		if _, err := strconv.Atoi(column); err == nil {
			continue //position based, not need to validate
		}

		if ns.View.Selector.Constraints.HasOrderByColumn(column) {
			continue
		}
		_, ok := ns.View.ColumnByName(column)
		if !ok {
			return fmt.Errorf("not found column %v at view %v", items, ns.View.Name)
		}
	}
	selector := s.state.Lookup(ns.View)
	selector.OrderBy = strings.Join(items, ",")
	return nil
}

func (s *Session) populateOffsetQuerySelector(ctx context.Context, ns *view.NamespaceView, opts *Options) error {
	selectorParameters := ns.View.Selector
	offsetParameters := ns.SelectorParameters(selectorParameters.OffsetParameter, view.QueryStateParameters.OffsetParameter)
	value, has, err := s.lookupFirstValue(ctx, offsetParameters, opts)
	if has && err == nil {
		err = s.setOffsetQuerySelector(value, ns)
	}
	return err
}

func (s *Session) setOffsetQuerySelector(value interface{}, ns *view.NamespaceView) error {
	if !ns.View.Selector.Constraints.Offset {
		return fmt.Errorf("can't use Offset on view %v", ns.View.Name)
	}
	selector := s.state.Lookup(ns.View)
	offset := value.(int)
	if offset <= ns.View.Selector.Limit || ns.View.Selector.Limit == 0 {
		selector.Offset = offset
	}
	return nil
}

func (s *Session) populateLimitQuerySelector(ctx context.Context, ns *view.NamespaceView, opts *Options) error {
	selectorParameters := ns.View.Selector
	limitParameters := ns.SelectorParameters(selectorParameters.LimitParameter, view.QueryStateParameters.LimitParameter)
	value, has, err := s.lookupFirstValue(ctx, limitParameters, opts)
	if has && err == nil {
		err = s.setLimitQuerySelector(value, ns)
	}
	return err
}

func (s *Session) setLimitQuerySelector(value interface{}, ns *view.NamespaceView) error {
	if !ns.View.Selector.Constraints.Limit {
		return fmt.Errorf("can't use Limit on view %v", ns.View.Name)
	}
	selector := s.state.Lookup(ns.View)
	limit, err := toInt(value)
	if err != nil {
		return fmt.Errorf("invalid limit value: %v", err)
	}
	if limit <= ns.View.Selector.Limit || ns.View.Selector.Limit == 0 {
		selector.Limit = limit
	}
	return nil
}

func (s *Session) populateFieldQuerySelector(ctx context.Context, ns *view.NamespaceView, opts *Options) error {
	selectorParameters := ns.View.Selector
	fieldParameters := ns.SelectorParameters(selectorParameters.FieldsParameter, view.QueryStateParameters.FieldsParameter)
	value, has, err := s.lookupFirstValue(ctx, fieldParameters, opts)
	if has && err == nil {
		err = s.setFieldsQuerySelector(value, ns)
	}
	return err
}

func (s *Session) setFieldsQuerySelector(value interface{}, ns *view.NamespaceView) (err error) {
	if !ns.View.Selector.Constraints.Projection {
		return fmt.Errorf("can't use projection on view %v", ns.View.Name)
	}
	selector := s.state.Lookup(ns.View)
	var fields []string
	switch v := value.(type) {
	case []string:
		fields = v
	case []interface{}:
		for _, elem := range v {
			text, ok := elem.(string)
			if !ok {
				continue
			}
			fields = append(fields, text)
		}
	}
	for _, field := range fields {
		fieldName := ns.View.CaseFormat.Format(field, text.CaseFormatUpperCamel)
		if err = canUseColumn(ns.View, fieldName); err != nil {
			return err
		}
		selector.Add(fieldName, ns.View.IsHolder(fieldName))
	}
	return nil
}

func (s *Session) setCriteriaQuerySelector(value interface{}, ns *view.NamespaceView) error {
	selector := s.state.Lookup(ns.View)
	switch actual := value.(type) {
	case string:
		if actual == "" {
			return nil
		}
		if !ns.View.Selector.Constraints.Criteria {
			return fmt.Errorf("can't use criteria on view %v", ns.View.Name)
		}
		sanitizedCriteria, err := criteria.Parse(actual, ns.View.IndexedColumns(), ns.View.Selector.Constraints.SqlMethodsIndexed())
		if err != nil {
			return err
		}
		selector.SetCriteria(sanitizedCriteria.Expression, sanitizedCriteria.Placeholders)
		return nil
	case *codec.Criteria:
		if actual == nil {
			return nil
		}
		selector.SetCriteria(actual.Expression, actual.Placeholders)
		return nil
	case codec.Criteria:
		selector.SetCriteria(actual.Expression, actual.Placeholders)
		return nil
	}
	return fmt.Errorf("unsupported ctieria type, %T", value)
}

func canUseColumn(aView *view.View, columnName string) error {
	_, ok := aView.ColumnByName(columnName)
	if !ok {
		return fmt.Errorf("not found column %v in view %v", columnName, aView.Name)
	}
	return nil
}

func toInt(v interface{}) (int, error) {
	switch val := v.(type) {
	case int:
		return val, nil
	case int32:
		return int(val), nil
	case int64:
		return int(val), nil
	case float64:
		return int(val), nil
	case float32:
		return int(val), nil
	default:
		return 0, fmt.Errorf("unsupported type: %T", v)
	}
}
