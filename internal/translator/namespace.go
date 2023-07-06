package translator

import (
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser/query"
)

type (
	Namespace struct {
		Name string
		SQL  string
		Join *query.Join
		OutputConfig
		Exclude    []string
		Transforms map[string]*Function
		View       *View
	}

	Function struct {
		Name string
		Args []string
	}

	OutputConfig struct {
		Style       string
		Field       string
		Kind        string
		Cardinality view.Cardinality
	}
)

func NewNamespace(name, SQL string) *Namespace {
	return &Namespace{
		Name:       name,
		SQL:        SQL,
		Join:       nil,
		Exclude:    nil,
		Transforms: map[string]*Function{},
		View:       &View{},
	}
}
