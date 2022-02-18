package binder

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/datly/v0/base/contract"
	config2 "github.com/viant/datly/v0/config"
	data2 "github.com/viant/datly/v0/data"
	"github.com/viant/datly/v0/db"
	metric2 "github.com/viant/datly/v0/metric"
	shared2 "github.com/viant/datly/v0/shared"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	tdata "github.com/viant/toolbox/data"
	"strings"
)

//Service represents binding service
type Service interface {
	BuildDataPool(ctx context.Context, request contract.Request, view *data2.View, rule *config2.Rule, metrics *metric2.Metrics, sourceType ...string) (data2.Pool, error)
}

type service struct {
	db db.Service
}

func (s *service) BuildDataPool(ctx context.Context, request contract.Request, view *data2.View, rule *config2.Rule, metrics *metric2.Metrics, sourceType ...string) (data2.Pool, error) {
	var result = data2.Pool{}
	filter := indexFilter(sourceType)
	if len(filter) == 0 || filter[shared2.BindingQueryString] {
		config2.MergeValues(request.QueryParams, result)
	}
	if len(filter) == 0 || filter[shared2.BindingBodyData] {
		config2.MergeMap(request.Data, result)
	}
	if len(filter) == 0 || filter[shared2.BindingPath] {
		config2.MergeValues(request.PathParams, result)
	}
	var err error
	if len(view.Parameters) > 0 {

		var value interface{}
		for _, binding := range view.Parameters {
			switch binding.Type {
			case shared2.BindingDataView:
				if value, err = s.loadViewData(ctx, binding, result, rule, metrics); err != nil {
					return nil, err
				}
			case shared2.BindingHeader:
				value = request.Headers.Get(binding.From)
			case shared2.BindingBodyData:
				value = request.Data[binding.From]
			case shared2.BindingQueryString:
				value = request.QueryParams.Get(binding.From)
			case shared2.BindingDataPool:
				aMap := tdata.Map(result)
				value, _ = aMap.GetValue(binding.From)

			case shared2.BindingPath:
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

func (s *service) loadViewData(ctx context.Context, binding *data2.Parameter, dataPool data2.Pool, rule *config2.Rule, metrics *metric2.Metrics) (interface{}, error) {
	view, err := rule.View(binding.DataView)
	if err != nil {
		return nil, err
	}
	selector := view.Selector.Clone()
	SQL, parameters, sqlErr := view.BuildSQL(selector, dataPool)
	if shared2.IsLoggingEnabled() {
		fmt.Printf("=====ParametrizedSQL======\n%v, \n\tparams: %v, \n\tdataPool: %+v\n", SQL, parameters, dataPool)
	}
	if sqlErr != nil {
		return nil, sqlErr
	}
	manager, err := s.db.Manager(ctx, view.Connector)
	if err != nil {
		return nil, err
	}
	data := newBindingData(view.Selector.Columns)
	parametrized := &dsc.ParametrizedSQL{SQL: SQL, Values: parameters}
	query := metric2.NewQuery(view.Name, parametrized)
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
