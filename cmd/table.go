package cmd

import (
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/view"
	"strings"
)

type (
	Alias     string
	TableName string

	Table struct {
		Ref             string
		Inner           Columns
		InnerAlias      string
		HolderName      string
		NamespaceSource string
		Deps            map[Alias]TableName
		Columns         Columns
		Name            string
		SQL             string
		Relations       Relations
		option.ViewHint
		ViewHintJSON string
	}

	Columns []*Column
	Column  struct {
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

	Relations []*Relation
	Relation  struct {
		Owner *Table
		option.ViewHint
		Table *Table
	}
)

func (t *Table) HasStarExpr(alias string) bool {
	return t.Inner.StarExpr(alias) != nil
}

func NewTable(name string) *Table {
	return &Table{
		Name: name,
		Deps: map[Alias]TableName{},
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
		result[join.Table.HolderName] = join
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
