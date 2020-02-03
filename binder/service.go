package binder

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/datly/base/contract"
	"github.com/viant/datly/config"
	"github.com/viant/datly/data"
	"github.com/viant/datly/db"
	"github.com/viant/datly/metric"
	"github.com/viant/datly/shared"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	tdata "github.com/viant/toolbox/data"
	"strings"
)

//Service represents binding service
type Service interface {
	BuildDataPool(ctx context.Context, request contract.Request, view *data.View, rule *config.Rule, metrics *metric.Metrics, sourceType ...string) (data.Pool, error)
}

type service struct {
	db db.Service
}

func (s *service) BuildDataPool(ctx context.Context, request contract.Request, view *data.View, rule *config.Rule, metrics *metric.Metrics, sourceType ...string) (data.Pool, error) {
	var result = data.Pool{}
	filter := indexFilter(sourceType)
	if len(filter) == 0 || filter[shared.BindingQueryString] {
		config.MergeValues(request.QueryParams, result)
	}
	if len(filter) == 0 || filter[shared.BindingBodyData] {
		config.MergeMap(request.Data, result)
	}
	if len(filter) == 0 || filter[shared.BindingPath] {
		config.MergeValues(request.PathParams, result)
	}
	var err error
	if len(view.Bindings) > 0 {

		var value interface{}
		for _, binding := range view.Bindings {
			switch binding.Type {
			case shared.BindingDataView:
				if value, err = s.loadViewData(ctx, binding, result, rule, metrics); err != nil {
					return nil, err
				}
			case shared.BindingHeader:
				value = request.Headers.Get(binding.From)
			case shared.BindingBodyData:
				value = request.Data[binding.From]
			case shared.BindingQueryString:
				value = request.QueryParams.Get(binding.From)
			case shared.BindingDataPool:
				aMap := tdata.Map(result)
				value, _ = aMap.GetValue(binding.From)

			case shared.BindingPath:
				value = request.PathParams.Get(binding.From)
			default:
				return nil, errors.Errorf("unsupported bindingData source: %v", binding.Type)
			}

			hasValue := value != nil && toolbox.AsString(value) != ""
			if (!hasValue) && (binding.Default != nil) {
				value = binding.Default
				hasValue = true
			}
			if hasValue && binding.Expression != "" {
				value = strings.Replace(binding.Expression, "$value", toolbox.AsString(value), len(binding.Expression))
			}
			result[binding.Name] = value
		}
	}
	return result, nil
}

func indexFilter(sourceType []string) map[string]bool {
	whiteList := make(map[string]bool)
	for _, bindingType := range sourceType {
		whiteList[bindingType] = true
	}
	return whiteList
}

func (s *service) loadViewData(ctx context.Context, binding *data.Binding, dataPool data.Pool, rule *config.Rule, metrics *metric.Metrics) (interface{}, error) {
	view, err := rule.View(binding.DataView)
	if err != nil {
		return nil, err
	}
	selector := view.Selector.Clone()
	SQL, parameters, sqlErr := view.BuildSQL(selector, dataPool)
	if shared.IsLoggingEnabled() {
		fmt.Printf("=====SQL======\n%v, \n\tparams: %v, \n\tdataPool: %+v\n", SQL, parameters, dataPool)
	}
	if sqlErr != nil {
		return nil, sqlErr
	}
	manager, err := s.db.Manager(ctx, view.Connector)
	if err != nil {
		return nil, err
	}
	data := newBindingData(view.Selector.Columns)
	parametrizedSQL := &dsc.ParametrizedSQL{SQL: SQL, Values: parameters}
	query := metric.NewQuery(parametrizedSQL)
	readHandler := func(scanner dsc.Scanner) (toContinue bool, err error) {
		query.Increment()
		return s.fetchBindingData(scanner, data)
	}
	err = manager.ReadAllWithHandler(SQL, parameters, readHandler)
	query.SetFetchTime()
	metrics.AddQuery(query)
	return data.Data(), err
}

func (s *service) fetchBindingData(scanner dsc.Scanner, data *bindingData) (bool, error) {
	record := map[string]interface{}{}
	err := scanner.Scan(&record)
	if err == nil {
		data.Add(record)
	}
	return err == nil, err
}

//New creates a new binding service
func New(db db.Service) Service {
	return &service{
		db: db,
	}
}
