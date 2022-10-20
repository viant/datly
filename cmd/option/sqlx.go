package option

import (
	"github.com/viant/datly/view"
	"strings"
)

type (
	Alias     string
	TableName string

	/* SELECT events.* FROM (
		SELECT * from events e2
	) events
	JOIN (
		SELECT * from event_types typ
	) event_types ON events.event_type_id = event_types.id
	*/
	Table struct {
		Ref             string
		Inner           Columns
		InnerAlias      string //e2
		OuterAlias      string //events
		NamespaceSource string
		Deps            map[Alias]TableName
		Columns         Columns
		Name            string
		SQL             string
		Relations       Relations
		TableMeta
		ViewHint string
	}

	TableMeta struct {
		Connector         string
		Self              *view.SelfReference
		Cache             *view.Cache
		Warmup            map[string]interface{}
		DataViewParameter *view.Parameter `json:"-"`
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
		Comments string
	}

	TableParam struct {
		Table *Table
		Param *view.Parameter
	}

	Columns []*Column

	Relation struct {
		Owner *Table
		TableMeta

		ToOne bool
		Table *Table
	}

	Relations []*Relation
)

func (t *Table) HasStarExpr(alias string) bool {
	return t.Inner.StarExpr(alias) != nil
}

func NewTable(name string) *Table {
	return &Table{
		Name: name,
		Deps: map[Alias]TableName{},
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
		result[strings.ToLower(alias)] = c[i]
	}
	return result
}

func (j *Relations) Index() map[string]*Relation {
	var result = make(map[string]*Relation)
	for _, join := range *j {
		result[join.Table.OuterAlias] = join
	}

	return result
}

func (c Columns) Index() map[string]*Column {
	result := map[string]*Column{}

	for i := range c {
		result[c[i].Name] = c[i]
	}

	return result
}
