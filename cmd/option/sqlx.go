package option

import (
	"github.com/viant/datly/view"
)

type (
	Table struct {
		Ref           string
		StarExpr      bool
		Inner         Columns
		ColumnTypes   map[string]string
		InnerAlias    string
		InnerSQL      string
		Deps          map[string]string
		Columns       Columns
		Name          string
		SQL           string
		Joins         Joins
		Alias         string
		ParamDataType string
		TableMeta
		ViewMeta *ViewMeta
		ViewHint string
	}

	TableMeta struct {
		Connector         string
		Cache             *view.Cache
		Warmup            map[string]interface{}
		DataViewParameter *view.Parameter `json:"-"`
		Parameter         *view.Parameter
		Auth              string
		Selector          *view.Config
		AllowNulls        *bool
	}

	Column struct {
		Ns       string
		Name     string
		Alias    string
		Except   []string
		DataType string
	}

	TableParam struct {
		Table *Table
		Param *view.Parameter
	}

	Columns []*Column

	Join struct {
		Key      string
		KeyAlias string
		OwnerKey string
		OwnerNs  string
		Owner    *Table
		TableMeta
		Field string

		ToOne bool
		Table *Table
	}

	Joins []*Join
)

func NewTable() *Table {
	return &Table{
		ColumnTypes: map[string]string{},
		Deps:        map[string]string{},
		TableMeta: TableMeta{
			Warmup: map[string]interface{}{},
		},
	}
}

func (c Columns) StarExpr(ns string) *Column {
	for _, item := range c {
		if item.Name == "*" && item.Ns == ns {
			return item
		}
	}
	return nil
}

func (c Columns) ByNs(ns string) map[string]*Column {
	var result = make(map[string]*Column)
	for i, item := range c {
		if item.Name == "*" || item.Ns != ns {
			continue
		}
		alias := item.Alias
		if alias == "" {
			alias = item.Name
		}
		result[alias] = c[i]
	}
	return result
}

func (c Columns) ByAlias() map[string]*Column {
	var result = make(map[string]*Column)
	if c == nil {
		return result
	}
	for i, item := range c {
		if item.Name == "*" {
			continue
		}
		alias := item.Alias
		if alias == "" {
			alias = item.Name
		}
		result[alias] = c[i]
	}
	return result
}

func (j *Joins) Index() map[string]*Join {
	var result = make(map[string]*Join)
	for _, join := range *j {
		result[join.Table.Alias] = join
	}

	return result
}
