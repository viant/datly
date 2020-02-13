package reader

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/datly/base"
	"github.com/viant/datly/base/contract"
	"github.com/viant/datly/config"
	"github.com/viant/datly/data"
	"github.com/viant/datly/generic"
	"github.com/viant/datly/metric"
	"github.com/viant/datly/shared"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"sync"
	"time"
)

//Service represents a reader service
type Service interface {
	Read(ctx context.Context, request *Request) *Response
}

type service struct {
	base.Service
}

//Read reads data for matched request Path
func (s *service) Read(ctx context.Context, request *Request) *Response {
	response := NewResponse()
	response.CreateTime = request.EventTime
	defer response.OnDone()
	if shared.IsLoggingEnabled() {
		toolbox.Dump(request)
	}
	err := s.read(ctx, request, response)
	if err != nil {
		response.AddError(shared.ErrorTypeException, "service.Read", err)
	}
	if shared.IsLoggingEnabled() {
		toolbox.Dump(response)
	}
	return response
}

func (s *service) read(ctx context.Context, req *Request, resp *Response) error {
	rule, err := s.Match(ctx, &req.Request, &resp.Response)
	if rule == nil {
		return err
	}
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(len(rule.Output))

	for i := range rule.Output {
		go func(io *data.IO) {
			defer waitGroup.Done()
			err := s.readOutputData(ctx, rule, io, req, resp)
			if err != nil {
				resp.AddError(shared.ErrorTypeException, "service.readOutputData", err)
			}
		}(rule.Output[i])
	}
	waitGroup.Wait()
	return nil
}

func (s *service) readOutputData(ctx context.Context, rule *config.Rule, io *data.IO, request *Request, response *Response) error {
	view, err := rule.View(io.DataView)
	if err != nil {
		return err
	}
	selector := view.Selector.Clone()
	genericProvider := generic.NewProvider()
	collection := genericProvider.NewArray()
	if io.OmitEmpty {
		selector.OmitEmpty = io.OmitEmpty
		collection.Proto().SetOmitEmpty(io.OmitEmpty)
	}

	selector.CaseFormat = io.CaseFormat
	err = s.readViewData(ctx, collection, selector, view, rule, request, response)
	if err == nil {
		io.SetOutput(collection, response)
	}
	return err
}

func (s *service) readViewData(ctx context.Context, collection generic.Collection, selector *data.Selector, view *data.View, rule *config.Rule, request *Request, response *Response) error {
	dataPool, err := s.BuildDataPool(ctx, request.Request, view, rule, response.Metrics)
	if err != nil {
		return errors.Wrapf(err, "failed to assemble bindingData with rule: %v", rule.Info.URL)
	}
	selector.Apply(dataPool)
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(1 + len(view.Refs))
	refData := &contract.Collections{}
	go s.readRefs(ctx, view, selector, dataPool, rule, request, response, waitGroup, refData)
	SQL, parameters, err := view.BuildSQL(selector, dataPool)
	if err != nil {
		return errors.Wrapf(err, "failed to build FromFragments with rule: %v", rule.Info.URL)
	}

	if shared.IsLoggingEnabled() {
		fmt.Printf("=====ParametrizedSQL======\n%v, \nparams: %v, dataPool: %+v\n", SQL, parameters, dataPool)
	}
	parametrizedSQL := &dsc.ParametrizedSQL{SQL: SQL, Values: parameters}
	query := metric.NewQuery(view.Name, parametrizedSQL)
	err = s.readData(ctx, view, query, collection, response)

	query.SetFetchTime()
	response.Metrics.AddQuery(query)
	if err != nil {
		return errors.Wrapf(err, "failed to read data with rule: %v", rule.Info.URL)
	}
	if selector.CaseFormat != view.CaseFormat {
		collection.Proto().OutputCaseFormat(view.CaseFormat, selector.CaseFormat)
	}
	waitGroup.Wait()
	if len(refData.Data) > 0 {

		s.assignRefs(view, collection, refData.Data)
	}
	if view.OnRead != nil {
		context := data.NewContext(ctx, view, s)
		collection.Objects(func(item *generic.Object) (toContinue bool, err error) {
			return view.OnRead.Visit(context, data.NewValue(item, nil))
		})
	}
	return err
}

func (s *service) readData(ctx context.Context, view *data.View, query *metric.Query, collection generic.Collection, response *Response) error {
	useCache := view.Cache != nil
	var key string
	parametrized := query.ParametrizedSQL()
	if useCache {
		key = shared.GetKey(view.Name, query.ParametrizedSQL())
		hit, err := s.readDataFromCache(ctx, key, view, query, collection)
		if err != nil {
			response.AddError(shared.ErrorTypeCache, "readData", err)
		}
		if hit {
			return nil
		}
	}
	manager, err := s.Manager(ctx, view.Connector)
	if err != nil {
		return err
	}

	var record *Record

	proto := collection.Proto()

	err = manager.ReadAllWithHandler(parametrized.SQL, parametrized.Values, func(scanner dsc.Scanner) (toContinue bool, err error) {
		if record == nil {
			columns, _ := scanner.Columns()
			columnTypes, _ := scanner.ColumnTypes()
			record = NewRecord(proto, columns, columnTypes)
		}
		record.Reset()
		err = scanner.Scan(record.valuePointers...)
		if err != nil {
			return false, err
		}
		query.Increment()
		object, err := record.Object()
		if err != nil {
			return false, err
		}
		collection.AddObject(object)
		return err == nil, err
	})

	if err == nil && useCache {
		if err := s.updateCache(ctx, collection, view, key); err != nil {
			response.AddError(shared.ErrorTypeCache, "updateCache", err)
		}
	}
	return err
}

func (s *service) updateCache(ctx context.Context, collection generic.Collection, view *data.View, key string) error {
	compacted := collection.Compact()
	compacted.TransformBinary()
	JSON, err := json.Marshal(compacted)
	if err == nil {
		err = view.Cacher().Put(ctx, key, JSON, view.Cache.TTL)
	}
	return err
}

func (s *service) readDataFromCache(ctx context.Context, key string, view *data.View, query *metric.Query, collection generic.Collection) (bool, error) {
	now := time.Now()
	defer query.SetCacheGetTime(now)
	cached, err := view.Cacher().Get(ctx, key)
	if err != nil {
		return false, err
	}
	if len(cached) == 0 {
		query.CacheMiss = true
		return false, nil
	}
	compacted := &generic.Compatcted{}
	if err := json.Unmarshal(cached, &compacted); err != nil {
		return false, errors.Wrapf(err, "failed to decode cache entry for key: %s", key)
	}
	compacted.Update(collection)
	query.CacheHit = true
	query.Count = uint32(collection.Size())
	query.SetFetchTime()
	return true, nil
}

func (s *service) readRefs(ctx context.Context, owner *data.View, selector *data.Selector, bindings map[string]interface{}, rule *config.Rule, request *Request, response *Response, group *sync.WaitGroup, refData *contract.Collections) {
	defer group.Done()
	refs := owner.Refs
	if len(refs) == 0 {
		return
	}

	for i, ref := range refs {
		if !selector.IsSelected(ref.Columns()) { //when selector comes with columns, make sure that reference is within that list.
			group.Done()
			continue
		}
		go s.readRefData(ctx, owner, refs[i], selector, bindings, response, rule, request, refData, group)
	}
}

func (s *service) readRefData(ctx context.Context, owner *data.View, ref *data.Reference, selector *data.Selector, bindings map[string]interface{}, response *Response, rule *config.Rule, request *Request, refData *contract.Collections, group *sync.WaitGroup) {
	defer group.Done()
	view, err := s.buildRefView(owner.Clone(), ref, selector, bindings)
	if err != nil {
		response.AddError(shared.ErrorTypeException, "service.readOutputData", err)
		return
	}
	provider := generic.NewProvider()
	var collection generic.Collection
	if ref.Cardinality == shared.CardinalityOne {
		collection = provider.NewMap(ref.RefIndex())
	} else {
		collection = provider.NewMultimap(ref.RefIndex())
	}

	if selector.OmitEmpty {
		collection.Proto().SetOmitEmpty(selector.OmitEmpty)
	}

	refViewSelector := view.Selector.Clone()
	if refViewSelector.CaseFormat == "" {
		refViewSelector.CaseFormat = selector.CaseFormat
	}
	err = s.readViewData(ctx, collection, refViewSelector, view, rule, request, response)
	if err != nil {
		response.AddError(shared.ErrorTypeException, "service.readViewData", err)
	}
	refData.Put(ref.Name, collection)
}

func (s *service) buildRefView(owner *data.View, ref *data.Reference, selector *data.Selector, bindings map[string]interface{}) (*data.View, error) {
	refView := ref.View()
	if refView == nil {
		return nil, errors.Errorf("ref view was empty for owner: %v", owner.Name)
	}
	refView = refView.Clone()
	//Only when owner and reference connector is the same you can apply JOIN, otherwise all reference table has to be read into memory.
	if refView.Connector == owner.Connector {
		selector = selector.Clone()
		selector.Columns = ref.Columns()
		SQL, parameters, err := owner.BuildSQL(selector, bindings)
		if err != nil {
			return nil, err
		}
		refView.Params = parameters
		join := &data.Join{
			Type:  shared.JoinTypeInner,
			Alias: ref.Alias(),
			Table: fmt.Sprintf("(%s)", SQL),
			On:    ref.Criteria(refView.Alias),
		}
		refView.AddJoin(join)
	}
	return refView, nil
}

func (s *service) assignRefs(owner *data.View, ownerCollection generic.Collection, refData map[string]generic.Collection) error {

	return ownerCollection.Objects(func(item *generic.Object) (b bool, err error) {
		for _, ref := range owner.Refs {
			if owner.HideRefIDs {
				for _, column := range ref.Columns() {
					ownerCollection.Proto().Hide(column)
				}
			}
			data, ok := refData[ref.Name]
			if !ok {
				continue
			}
			index := ref.Index()
			key := index(item)

			if ref.Cardinality == shared.CardinalityOne {
				aMap, ok := data.(*generic.Map)
				if !ok {
					return false, errors.Errorf("invalid collection: expected : %T, but had %T", aMap, data)
				}
				value := aMap.Object(key)
				item.SetValue(ref.Name, value)
			} else {
				aMultimap, ok := data.(*generic.Multimap)
				if !ok {
					return false, errors.Errorf("invalid collection: expected : %T, but had %T", aMultimap, data)
				}
				value := aMultimap.Slice(key)
				item.SetValue(ref.Name, value)
			}
		}
		return true, nil
	})
}

//New creates a service
func New(ctx context.Context, config *config.Config) (Service, error) {
	baseService, err := base.New(ctx, config)
	if err != nil {
		return nil, err
	}
	return &service{
		Service: baseService,
	}, err
}
