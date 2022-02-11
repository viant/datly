package data

import "reflect"

type Selector interface {
	GetType() reflect.Type
	GetColumns() []*Column
	GetOrderBy() string
	GetOffset() int
	GetLimit() int
}

type ClientSelector struct {
	columns []*Column
	OrderBy string
	Offset  int
	Limit   int
	rType   reflect.Type
}

func (c *ClientSelector) GetType() reflect.Type {
	return c.rType
}

func (c *ClientSelector) GetColumns() []*Column {
	return c.columns
}

func (c *ClientSelector) GetOrderBy() string {
	return c.OrderBy
}

func (c *ClientSelector) GetOffset() int {
	return c.Offset
}

func (c *ClientSelector) GetLimit() int {
	return c.Limit
}
