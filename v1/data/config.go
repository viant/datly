package data

import (
	"github.com/viant/datly/data"
)

//Config represent a data selector for projection and selection
type Config struct {
	Columns []string `json:",omitempty"`

	//TODO: Should order by be a slice?
	OrderBy  string         `json:",omitempty"`
	Limit    int            `json:",omitempty"`
	Criteria *data.Criteria `json:",omitempty"`
}

func (c *Config) GetOrderBy() string {
	return c.OrderBy
}

func (c *Config) GetOffset() int {
	return 0
}

func (c *Config) GetLimit() int {
	return c.Limit
}
