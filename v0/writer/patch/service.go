package patch

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/datly/v0/base"
	"github.com/viant/datly/v0/base/contract"
	config2 "github.com/viant/datly/v0/config"
	data2 "github.com/viant/datly/v0/data"
	"github.com/viant/datly/v0/metric"
	shared2 "github.com/viant/datly/v0/shared"
	writer2 "github.com/viant/datly/v0/writer"
	db2 "github.com/viant/datly/v0/writer/db"
	"github.com/viant/dsc"
	"github.com/viant/gtly"
	"github.com/viant/toolbox"
	"sync"
)

//Service represents view service
type Service interface {
	Patch(ctx context.Context, request *Request) *writer2.Response
}

type service struct {
	base.Service
}

func (p *service) Patch(ctx context.Context, request *Request) *writer2.Response {
	response := writer2.NewResponse()
	defer response.OnDone()
	if shared2.IsLoggingEnabled() {
		toolbox.Dump(request)
	}
	err := p.patch(ctx, request, response)
	if err != nil {
		response.AddError(shared2.ErrorTypeException, "writer.Patch", err)
	}
	if shared2.IsLoggingEnabled() {
		toolbox.Dump(response)
	}
	return response
}

func (p *service) patch(ctx context.Context, req *Request, resp *writer2.Response) error {
	rule, err := p.Match(ctx, &req.Request, &resp.Response)
	if rule == nil {
		return err
	}
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(len(rule.Input))

	patched := contract.NewData()
	for i := range rule.Input {
		go func(io *data2.IO) {
			defer waitGroup.Done()
			err = p.writeInputData(ctx, rule, io, req, resp, patched)
			if err != nil {
				resp.AddError(shared2.ErrorTypeException, "service.writeInputData", err)
			}
		}(rule.Input[i])
	}
	waitGroup.Wait()
	for _, io := range rule.Output {
		collection := patched.Get(io.DataView)
		if collection == nil {
			continue
		}
		io.SetOutput(collection, resp)
	}
	return nil
}

func (p *service) writeInputData(ctx context.Context, rule *config2.Rule, io *data2.IO, req *Request, resp *writer2.Response, patched *contract.Collections) error {
	view, err := rule.View(io.DataView)
	if err != nil {
		return err
	}
	if !view.IsMutable() {
		return errors.Errorf("view view: %v is immutable", view.Name)
	}
	collection, err := writer2.NewCollection(req.Data, view, io)
	if err != nil {
		return errors.Wrapf(err, "failed to build collection for view view: %v", view.Name)
	}
	patched.Put(view.Name, collection)
	var filterTypes = make([]string, 0)
	if len(view.Parameters) == 0 { //if binding specified  use explicit binding only
		filterTypes = append(filterTypes, shared2.BindingPath)
	}
	dataPool, err := p.BuildDataPool(ctx, req.Request, view, rule, resp.Metrics, filterTypes...)
	if err != nil {
		return errors.Wrapf(err, "failed to build view pool for view view: %v", view.Name)
	}

	collection.Objects(func(item *gtly.Object) (toContinue bool, err error) {
		//TODO check with specified, view type validation, date formatting, beforePath visitor call
		for k, v := range dataPool {
			item.SetValue(k, v)
		}
		return true, nil
	})

	return p.patchDataView(ctx, view, collection, dataPool, req, resp.Metrics)
}

func (p *service) patchDataView(ctx context.Context, view *data2.View, collection gtly.Collection, dataPool data2.Pool, request *Request, metrics *metric.Metrics) (err error) {
	manager, err := p.Manager(ctx, view.Connector)
	if err != nil {
		return err
	}
	dbConn, connErr := manager.ConnectionProvider().Get()
	if connErr != nil {
		return errors.Wrapf(connErr, "failed to connection for patching %v", view.Table)
	}
	inTransaction := false
	defer func() {
		if inTransaction {
			if err != nil {
				_ = dbConn.Rollback()
			} else {
				_ = dbConn.Commit()
			}
		}
		_ = dbConn.Close()
	}()
	indexer := db2.NewIndexer(view)
	index := indexer.Index(collection)
	if checkErr := p.removeNonExisting(ctx, manager, dbConn, view, index, metrics); checkErr != nil {
		return errors.Wrapf(checkErr, "failed to index existing record on %v", view.Table)
	}

	err = dbConn.Begin()
	if err != nil {
		return errors.Wrapf(err, "failed to open transaction on %v", view.Table)
	}
	inTransaction = true
	if len(index) != collection.Size() {
		if err = p.insertData(collection, index, view, manager, dbConn, metrics); err != nil {
			return err
		}
	}
	if len(index) == 0 { //nothing to update
		return nil
	}
	err = p.updateData(collection, indexer, index, view, manager, dbConn, metrics)
	return err
}

func (p *service) updateData(collection gtly.Collection, indexer *db2.Indexer, index map[string][]interface{}, view *data2.View, manager dsc.Manager, dbConn dsc.Connection, metrics *metric.Metrics) error {
	updatable := db2.Newupdatable(collection, indexer, index)
	update := db2.NewUpdate(view)
	keySetter := db2.NewKeySetter(view)

	_, err := manager.PersistData(dbConn, updatable, view.Table, keySetter, update.DML)
	if len(update.Queries) > 0 {
		for i := range update.Queries {
			update.Queries[i].SetExecutionTime()
			metrics.AddQuery(update.Queries[i])
		}
	}
	return err
}

func (p *service) insertData(collection gtly.Collection, index map[string][]interface{}, view *data2.View, manager dsc.Manager, dbConn dsc.Connection, metrics *metric.Metrics) error {
	keySetter := db2.NewKeySetter(view)
	indexer := db2.NewIndexer(view)

	insertable := db2.NewInsertable(collection, indexer, index)
	insert := db2.NewInsert(view)
	_, err := manager.PersistData(dbConn, insertable, view.Table, keySetter, insert.DML)
	if insert.Query != nil {
		insert.Query.SetExecutionTime()
		metrics.AddQuery(insert.Query)
	}
	if err != nil {
		return errors.Wrapf(err, "failed to insert view to %v", view.Table)
	}
	return nil
}

//New creates a service
func New(ctx context.Context, config *config2.Config) (Service, error) {
	baseService, err := base.New(ctx, config)
	if err != nil {
		return nil, err
	}
	return &service{
		Service: baseService,
	}, err
}
