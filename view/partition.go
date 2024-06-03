package view

import (
	"context"
	"database/sql"
	"reflect"
)

// Partitioned represents a partitioned view
type Partitioned struct {
	DataType    string
	Arguments   []string
	Concurrency int
	partitioner Partitioner
}

// SetPartitioner sets the partitioner
func (p *Partitioned) SetPartitioner(partitioner Partitioner) {
	p.partitioner = partitioner
}

// Partitioner returns the partitioner
func (p *Partitioned) Partitioner() Partitioner {
	return p.partitioner
}

// Partitioner represents a partitioner
type Partitioner interface {
	Partitions(ctx context.Context, db *sql.DB, aView *View) *Partitions
}

// ReducerProvider represents a reducer provider
type ReducerProvider interface {
	Reducer(ctx context.Context) Reducer
}

// Reducer represents a reducer
type Reducer interface {
	Reduce(slice interface{}) interface{}
}

// Partitions represents a partitioned view
type Partitions struct {
	Table      string
	Expression string
	Partitions [][]interface{}
	Partition  []interface{}
}

// NewPartitioned creates a new partitioned view
func NewPartitioned(paritioner Partitioner, concurrency int, args ...string) *Partitioned {
	return &Partitioned{DataType: reflect.TypeOf(paritioner).Name(), partitioner: paritioner, Concurrency: concurrency, Arguments: args}
}
