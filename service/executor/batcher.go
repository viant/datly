package executor

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/io/insert/batcher"
	"reflect"
	"sync"
)

type (
	BatcherRegistry struct {
		sync.Mutex
		tableNamesIndex    map[string]int
		tableTypesRegistry []*BatcherTypeRegistry
	}

	BatcherTypeRegistry struct {
		sync.Mutex
		tableTypesIndex map[reflect.Type]int
		batchServices   []*batcher.Service
	}
)

func NewBatcherRegistry() *BatcherRegistry {
	return &BatcherRegistry{
		tableNamesIndex: map[string]int{},
	}
}

func (r *BatcherRegistry) GetBatcher(tableName string, rType reflect.Type, db *sql.DB, config *batcher.Config) (*batcher.Service, error) {
	var batcherTypeRegistry *BatcherTypeRegistry
	r.Lock()
	batcherTypeRegistry = r.getBatcherTypeRegistryWithouLock(tableName)
	r.Unlock()

	return batcherTypeRegistry.GetBatcher(tableName, rType, db, config)
}

func (r *BatcherRegistry) getBatcherTypeRegistryWithouLock(tableName string) *BatcherTypeRegistry {
	registryIndex, ok := r.tableNamesIndex[tableName]
	if ok {
		return r.tableTypesRegistry[registryIndex]
	}

	registry := NewBatcherTypeRegistry()
	r.tableNamesIndex[tableName] = len(r.tableTypesRegistry)
	r.tableTypesRegistry = append(r.tableTypesRegistry, registry)
	return registry
}

func NewBatcherTypeRegistry() *BatcherTypeRegistry {
	return &BatcherTypeRegistry{
		Mutex:           sync.Mutex{},
		tableTypesIndex: map[reflect.Type]int{},
	}
}

func (r *BatcherTypeRegistry) GetBatcher(tableName string, rType reflect.Type, db *sql.DB, config *batcher.Config) (*batcher.Service, error) {
	var aBatcher *batcher.Service
	var err error
	r.Mutex.Lock()
	aBatcher, err = r.getBatcherWithouLock(tableName, rType, db, config)
	r.Mutex.Unlock()

	return aBatcher, err
}

func (r *BatcherTypeRegistry) getBatcherWithouLock(tableName string, rType reflect.Type, db *sql.DB, config *batcher.Config) (*batcher.Service, error) {
	batcherIndex, ok := r.tableTypesIndex[rType]
	if ok {
		return r.batchServices[batcherIndex], nil
	}

	inserter, err := insert.New(context.Background(), db, tableName)
	if err != nil {
		return nil, err
	}

	aBatcher, err := batcher.New(context.Background(), inserter, rType, config)
	if err != nil {
		return nil, err
	}

	r.tableTypesIndex[rType] = len(r.batchServices)
	r.batchServices = append(r.batchServices, aBatcher)
	return aBatcher, nil
}
