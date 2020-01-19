package data

import (
	"fmt"
	"github.com/go-errors/errors"
	"github.com/viant/toolbox/data"
	"strings"
)

//View represents a data view
type View struct {
	Connector     string
	Name          string
	Alias         string
	Table         string        `json:",omitempty"`
	From          string        `json:",omitempty"`
	FromURL       string        `json:",omitempty"`
	Columns       []*Column     `json:",omitempty"`
	Bindings      []*Binding    `json:",omitempty"`
	Criteria      *Criteria     `json:",omitempty"`
	Selector      Selector      `json:",omitempty"`
	Joins         []*Join       `json:",omitempty"`
	Refs          []*Reference  `json:",omitempty"`
	Params        []interface{} `json:",omitempty"`
}

//AddJoin add join
func (v *View) AddJoin(join *Join) {
	v.Joins = append(v.Joins, join)

}

//Clone creates a view clone
func (v *View) Clone() *View {
	return &View{
		Connector: v.Connector,
		Name:      v.Name,
		Alias:     v.Alias,
		Table:     v.Table,
		From:      v.From,
		FromURL:   v.FromURL,
		Columns:   v.Columns,
		Bindings:  v.Bindings,
		Criteria:  v.Criteria,
		Selector:  v.Selector,
		Refs:      v.Refs,
		Params:    v.Params,
	}
}

//Validate checks if view is valid
func (v View) Validate() error {
	if v.Table == "" && v.From == "" {
		return errors.Errorf("table was empty")
	}
	if v.Connector == "" {
		return errors.Errorf("connector was empty")
	}
	if len(v.Bindings) > 0 {
		for i := range v.Bindings {
			if err := v.Bindings[i].Validate(); err != nil {
				return err
			}
		}
	}
	if len(v.Refs) > 0 {
		for i := range v.Refs {
			if err := v.Refs[i].Validate(); err != nil {
				return err
			}
		}
	}
	return nil
}

//Init intializes view
func (v *View) Init() {
	if v.Name == "" && v.Table != "" {
		v.Name = v.Table
	}
	if v.Alias == "" {
		v.Alias = "t"
	}

	if len(v.Bindings) > 0 {
		for i := range v.Bindings {
			v.Bindings[i].Init()
		}
	}
}

const (
	projectionKey = "projection"
	fromKey       = "from"
	aliasKey      = "alias"
	criteriaKey   = "criteria"
	joinsKey      = "joins"
	limitKey      = "limit"
	orderByKey    = "orderBy"
	sqlTemplate   = `SELECT $projection 
FROM $from ${alias}${joins}${criteria}${orderBy}${limit}`
)

func (v View) BuildSQL(selector *Selector, bindings map[string]interface{}) (string, []interface{}, error) {
	projection := v.buildSQLProjection(selector)
	from := v.buildSQLFom(bindings)
	orderBy := v.buildSQLOrderBy(selector)
	criteria, parameters := v.buildSQLCriteria(selector, bindings)
	limit := v.buildSQLLimit(selector, bindings)
	joins := v.buildSQLJoins(selector, bindings)

	var replacements = data.NewMap()
	replacements.Put(projectionKey, projection)
	replacements.Put(fromKey, from)
	replacements.Put(aliasKey, v.Alias)
	replacements.Put(criteriaKey, criteria)
	replacements.Put(limitKey, limit)
	replacements.Put(orderByKey, orderBy)
	replacements.Put(joinsKey, joins)
	SQL := replacements.ExpandAsText(sqlTemplate)
	return SQL, parameters, nil
}

func (v View) buildSQLFom(bindings data.Map) string {
	from := v.Table
	if v.From != "" {
		from = "(" + v.From + ")"
	}
	return bindings.ExpandAsText(from)
}


func (v View) buildSQLProjection(selector *Selector) string {
	projection := v.Alias + ".*"


	if len(selector.Columns) > 0 {
		var columns = make([]string, 0)
		for i := range selector.Columns {
			columns = append(columns, fmt.Sprintf("\t%v.%v", v.Alias, selector.Columns [i]))
		}
		projection = strings.Join(columns, ",\n")
	}
	return projection
}

func (v View) buildSQLOrderBy(selector *Selector) string {
	if selector.OrderBy == "" {
		return ""
	}
	return "\nORDER BY " + selector.OrderBy
}

func (v View) buildSQLCriteria(selector *Selector, bindings data.Map) (string, []interface{}) {
	result := ""
	if v.Criteria == nil && selector.Criteria == nil {
		return result, nil
	}
	var parameters = make([]interface{}, 0)
	if v.Criteria != nil {
		result = "\nWHERE (" + bindings.ExpandAsText(v.Criteria.Expression) + ")"
		parameters = addCriteriaParams(v.Criteria.Params, bindings, parameters)
	}

	if selector.Criteria == nil {
		return result, parameters
	}
	if result == "" {
		result += "\nWHERE "
	} else {
		result += "\n AND "
	}
	result += "(" + bindings.ExpandAsText(selector.Criteria.Expression) + ")"
	parameters = addCriteriaParams(selector.Criteria.Params, bindings, parameters)
	return result, parameters
}

func addCriteriaParams(nameParams []string, bindings data.Map, valueParams []interface{}) []interface{} {
	if len(nameParams) == 0 {
		return valueParams
	}
	for _, key := range nameParams {
		value, ok := bindings[key]
		if ! ok {
			value, _ = bindings.GetValue(key)
		}
		valueParams = append(valueParams, value)
	}
	return valueParams
}

func (v View) buildSQLLimit(selector *Selector, bindings map[string]interface{}) string {
	if selector.Limit == 0 && selector.Offset == 0 {
		return ""
	}
	result := ""
	if selector.Limit > 0 {
		result = fmt.Sprint(" LIMIT  ", selector.Limit)
	}
	if selector.Offset > 0 {
		result += fmt.Sprint(" OFFSET  ", selector.Offset)
	}
	return result
}

func (v *View) buildSQLJoins(selector *Selector, bindings map[string]interface{}) string {
	if len(v.Joins) == 0 {
		return ""
	}
	var joins = make([]string, 0)
	for _, join := range v.Joins {
		joins = append(joins, fmt.Sprintf(" %v JOIN %v %v ON %v", join.Type, join.Table, join.Alias, join.On))
	}
	return "\n" + strings.Join(joins, "\n")
}
