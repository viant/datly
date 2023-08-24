package session

import (
	"context"
	"fmt"
	"github.com/viant/datly/router/criteria"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/view"
	"github.com/viant/toolbox/format"
	"github.com/viant/xdatly/codec"
	"strconv"
	"strings"
)

func (s *State) setQuerySelector(ctx context.Context, ns *view.NamespaceView, opts *Options) (err error) {
	selectorParameters := ns.View.Selector
	if selectorParameters == nil {
		return nil
	}

	if err = s.populateFieldQuerySelector(ctx, ns, opts); err != nil {
		return httputils.NewParamError(ns.View.Name, selectorParameters.FieldsParameter.Name, err)
	}
	if err = s.populateLimitQuerySelector(ctx, ns, opts); err != nil {
		return httputils.NewParamError(ns.View.Name, selectorParameters.LimitParameter.Name, err)
	}
	if err = s.populateOffsetQuerySelector(ctx, ns, opts); err != nil {
		return httputils.NewParamError(ns.View.Name, selectorParameters.OffsetParameter.Name, err)
	}
	if err = s.populateOrderByQuerySelector(ctx, ns, opts); err != nil {
		return httputils.NewParamError(ns.View.Name, selectorParameters.OrderByParameter.Name, err)
	}
	if err = s.populateCriteriaQuerySelector(ctx, ns, opts); err != nil {
		return httputils.NewParamError(ns.View.Name, selectorParameters.CriteriaParameter.Name, err)
	}
	if err = s.populatePageQuerySelector(ctx, ns, opts); err != nil {
		return httputils.NewParamError(ns.View.Name, selectorParameters.PageParameter.Name, err)
	}

	selector := s.states.Lookup(ns.View)
	if selector.Limit == 0 && selector.Offset != 0 {
		return fmt.Errorf("can't use offset without limit - view: %v", ns.View.Name)
	}
	return nil
}

func (s *State) populatePageQuerySelector(ctx context.Context, ns *view.NamespaceView, opts *Options) error {
	selectorParameters := ns.View.Selector
	pageParameters := ns.SelectorParameters(selectorParameters.PageParameter, view.RootSelectors.PageParameter)
	value, has, err := s.lookupFirstValue(ctx, pageParameters, opts)
	if has && err == nil {
		err = s.setPageQuerySelector(value, ns)
	}
	return err
}

func (s *State) setPageQuerySelector(value interface{}, ns *view.NamespaceView) error {
	page := value.(int)
	selector := s.states.Lookup(ns.View)
	actualLimit := selector.Limit
	if actualLimit == 0 {
		actualLimit = ns.View.Selector.Limit
	}
	selector.Offset = actualLimit * (page - 1)
	selector.Limit = actualLimit
	selector.Page = page
	return nil
}

func (s *State) populateCriteriaQuerySelector(ctx context.Context, ns *view.NamespaceView, opts *Options) error {
	selectorParameters := ns.View.Selector
	criteriaParameters := ns.SelectorParameters(selectorParameters.CriteriaParameter, view.RootSelectors.CriteriaParameter)
	value, has, err := s.lookupFirstValue(ctx, criteriaParameters, opts)
	if has && err == nil {
		err = s.setCriteriaQuerySelector(value, ns)
	}
	return err
}

func (s *State) populateOrderByQuerySelector(ctx context.Context, ns *view.NamespaceView, opts *Options) error {
	selectorParameters := ns.View.Selector
	orderByParameters := ns.SelectorParameters(selectorParameters.OrderByParameter, view.RootSelectors.OrderByParameter)
	value, has, err := s.lookupFirstValue(ctx, orderByParameters, opts)
	if has && err == nil {
		err = s.setOrderByQuerySelector(value, ns)
	}
	return err
}

func (s *State) setOrderByQuerySelector(value interface{}, ns *view.NamespaceView) error {
	if !ns.View.Selector.Constraints.OrderBy {
		return fmt.Errorf("can't use orderBy on view %v", ns.View.Name)
	}
	columns := value.([]string)
	for _, column := range columns {
		if _, err := strconv.Atoi(column); err == nil {
			continue //position based, not need to validate
		}
		_, ok := ns.View.ColumnByName(column)
		if !ok {
			return fmt.Errorf("not found column %v at view %v", columns, ns.View.Name)
		}
	}
	selector := s.states.Lookup(ns.View)
	selector.OrderBy = strings.Join(columns, ",")
	return nil
}

func (s *State) populateOffsetQuerySelector(ctx context.Context, ns *view.NamespaceView, opts *Options) error {
	selectorParameters := ns.View.Selector
	offsetParameters := ns.SelectorParameters(selectorParameters.OffsetParameter, view.RootSelectors.OffsetParameter)
	value, has, err := s.lookupFirstValue(ctx, offsetParameters, opts)
	if has && err == nil {
		err = s.setOffsetQuerySelector(value, ns)
	}
	return err
}

func (s *State) setOffsetQuerySelector(value interface{}, ns *view.NamespaceView) error {
	if !ns.View.Selector.Constraints.Offset {
		return fmt.Errorf("can't use Offset on view %v", ns.View.Name)
	}
	selector := s.states.Lookup(ns.View)
	offset := value.(int)
	if offset <= ns.View.Selector.Limit || ns.View.Selector.Limit == 0 {
		selector.Offset = offset
	}
	return nil
}

func (s *State) populateLimitQuerySelector(ctx context.Context, ns *view.NamespaceView, opts *Options) error {
	selectorParameters := ns.View.Selector
	limitParameters := ns.SelectorParameters(selectorParameters.LimitParameter, view.RootSelectors.LimitParameter)
	value, has, err := s.lookupFirstValue(ctx, limitParameters, opts)
	if has && err == nil {
		err = s.setLimitQuerySelector(value, ns)
	}
	return err
}

func (s *State) setLimitQuerySelector(value interface{}, ns *view.NamespaceView) error {
	if !ns.View.Selector.Constraints.Limit {
		return fmt.Errorf("can't use Limit on view %v", ns.View.Name)
	}
	selector := s.states.Lookup(ns.View)
	limit := value.(int)
	if limit <= ns.View.Selector.Limit || ns.View.Selector.Limit == 0 {
		selector.Limit = limit
	}
	return nil
}

func (s *State) populateFieldQuerySelector(ctx context.Context, ns *view.NamespaceView, opts *Options) error {
	selectorParameters := ns.View.Selector
	fieldParameters := ns.SelectorParameters(selectorParameters.FieldsParameter, view.RootSelectors.FieldsParameter)
	value, has, err := s.lookupFirstValue(ctx, fieldParameters, opts)
	if has && err == nil {
		err = s.setFieldsQuerySelector(value, ns)
	}
	return err
}

func (s *State) setFieldsQuerySelector(value interface{}, ns *view.NamespaceView) (err error) {
	if !ns.View.Selector.Constraints.Projection {
		return fmt.Errorf("can't use projection on view %v", ns.View.Name)
	}
	selector := s.states.Lookup(ns.View)
	fields := value.([]string)
	for _, field := range fields {
		fieldName := ns.View.Caser.Format(field, format.CaseUpperCamel)
		if err = canUseColumn(ns.View, fieldName); err != nil {
			return err
		}
		selector.Add(fieldName, ns.View.IsHolder(fieldName))
	}
	return nil
}

func (s *State) setCriteriaQuerySelector(value interface{}, ns *view.NamespaceView) error {
	selector := s.states.Lookup(ns.View)
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
		selector.SetCriteria(actual.Predicate, actual.Args)
		return nil
	case codec.Criteria:
		selector.SetCriteria(actual.Predicate, actual.Args)
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
