package session

import (
	"context"
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/toolbox/format"
	"strconv"
	"strings"
)

func (s *State) populateQuerySelector(ctx context.Context, ns *view.NamespaceView) (err error) {
	selectorParameters := ns.View.Selector
	if selectorParameters == nil {
		return nil
	}
	if err = s.populateFieldQuerySelector(ctx, ns, selectorParameters); err != nil {
		return err
	}
	if err = s.populateLimitQuerySelector(ctx, ns, selectorParameters); err != nil {
		return err
	}
	if err = s.populateOffsetQuerySelector(ctx, ns, selectorParameters); err != nil {
		return err
	}
	if err = s.populateOrderByQuerySelector(ctx, ns, selectorParameters); err != nil {
		return err
	}

	if err = s.populateCriteriaQuerySelector(ctx, ns, selectorParameters); err != nil {
		return err
	}
	if err = s.populatePageQuerySelector(ctx, ns, selectorParameters); err != nil {
		return err
	}
	selector := s.Selectors.Lookup(ns.View)
	if selector.Limit == 0 && selector.Offset != 0 {
		return fmt.Errorf("can't use offset without limit - view: %v", ns.View.Name)
	}
	return nil
}

func (s *State) populatePageQuerySelector(ctx context.Context, ns *view.NamespaceView, selectorParameters *view.Config) error {
	pageParameters := ns.SelectorParameters(selectorParameters.PageParameter, view.RootSelectors.PageParameter)
	value, has, err := s.lookupFirstValue(ctx, pageParameters)
	if has && err == nil {
		err = s.setPageQuerySelector(value, ns)
	}
	return err
}

func (s *State) setPageQuerySelector(value interface{}, ns *view.NamespaceView) error {
	page := value.(int)
	selector := s.Selectors.Lookup(ns.View)
	actualLimit := selector.Limit
	if actualLimit == 0 {
		actualLimit = ns.View.Selector.Limit
	}
	selector.Offset = actualLimit * (page - 1)
	selector.Limit = actualLimit
	selector.Page = page
	return nil
}

func (s *State) populateCriteriaQuerySelector(ctx context.Context, ns *view.NamespaceView, selectorParameters *view.Config) error {
	criteriaParameters := ns.SelectorParameters(selectorParameters.CriteriaParameter, view.RootSelectors.CriteriaParameter)
	value, has, err := s.lookupFirstValue(ctx, criteriaParameters)
	if has && err == nil {
		err = s.setCriteriaQuerySelector(value, ns)
	}
	return err
}

func (s *State) populateOrderByQuerySelector(ctx context.Context, ns *view.NamespaceView, selectorParameters *view.Config) error {
	orderByParameters := ns.SelectorParameters(selectorParameters.OrderByParameter, view.RootSelectors.OrderByParameter)
	value, has, err := s.lookupFirstValue(ctx, orderByParameters)
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
	selector := s.Selectors.Lookup(ns.View)
	selector.OrderBy = strings.Join(columns, ",")
	return nil
}

func (s *State) populateOffsetQuerySelector(ctx context.Context, ns *view.NamespaceView, selectorParameters *view.Config) error {
	offsetParameters := ns.SelectorParameters(selectorParameters.OffsetParameter, view.RootSelectors.OffsetParameter)
	value, has, err := s.lookupFirstValue(ctx, offsetParameters)
	if has && err == nil {
		err = s.setOffsetQuerySelector(value, ns)
	}
	return err
}

func (s *State) setOffsetQuerySelector(value interface{}, ns *view.NamespaceView) error {
	if !ns.View.Selector.Constraints.Offset {
		return fmt.Errorf("can't use Offset on view %v", ns.View.Name)
	}
	selector := s.Selectors.Lookup(ns.View)
	offset := value.(int)
	if offset <= ns.View.Selector.Limit || ns.View.Selector.Limit == 0 {
		selector.Offset = offset
	}
	return nil
}

func (s *State) populateLimitQuerySelector(ctx context.Context, ns *view.NamespaceView, selectorParameters *view.Config) error {
	limitParameters := ns.SelectorParameters(selectorParameters.LimitParameter, view.RootSelectors.LimitParameter)
	value, has, err := s.lookupFirstValue(ctx, limitParameters)
	if has && err == nil {
		err = s.setLimitQuerySelector(value, ns)
	}
	return err
}

func (s *State) setLimitQuerySelector(value interface{}, ns *view.NamespaceView) error {
	if !ns.View.Selector.Constraints.Limit {
		return fmt.Errorf("can't use Limit on view %v", ns.View.Name)
	}
	selector := s.Selectors.Lookup(ns.View)
	limit := value.(int)
	if limit <= ns.View.Selector.Limit || ns.View.Selector.Limit == 0 {
		selector.Limit = limit
	}
	return nil
}

func (s *State) populateFieldQuerySelector(ctx context.Context, ns *view.NamespaceView, selectorParameters *view.Config) error {
	fieldParameters := ns.SelectorParameters(selectorParameters.FieldsParameter, view.RootSelectors.FieldsParameter)
	value, has, err := s.lookupFirstValue(ctx, fieldParameters)
	if has && err == nil {
		err = s.setFieldsQuerySelector(value, ns)
	}
	return err
}

func (s *State) setFieldsQuerySelector(value interface{}, ns *view.NamespaceView) (err error) {
	if !ns.View.Selector.Constraints.Projection {
		return fmt.Errorf("can't use projection on view %v", ns.View.Name)
	}
	selector := s.Selectors.Lookup(ns.View)
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

	return nil
}

func canUseColumn(aView *view.View, columnName string) error {
	_, ok := aView.ColumnByName(columnName)
	if !ok {
		return fmt.Errorf("not found column %v in view %v", columnName, aView.Name)
	}
	return nil
}
