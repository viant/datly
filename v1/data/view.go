package data

import (
	"github.com/viant/datly/data"
)

//View represents a data view
type View struct {
	Connector string
	Name      string
	Alias     string    `json:",omitempty"`
	Table     string    `json:",omitempty"`
	From      string    `json:",omitempty"`
	Columns   []*Column `json:",omitempty"`

	Criteria *data.Criteria `json:",omitempty"`
	Selector Selector       `json:",omitempty"`

	PrimaryKey []string `json:",omitempty"`
	Mutable    *bool    `json:",omitempty"`
	Component  *Component
}
