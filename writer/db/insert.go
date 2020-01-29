package db

import (
	"fmt"
	"github.com/viant/datly/data"
	"github.com/viant/datly/generic"
	"github.com/viant/datly/metric"
	"github.com/viant/dsc"
	"strings"
)

const insertTemplate = "INSERT INTO %v(%v) VALUES(%v)"

type Insert struct {
	Query *metric.Query
	view  *data.View
	_SQL  string
}

func (i *Insert) DML(item interface{}) *dsc.ParametrizedSQL {
	obj := item.(*generic.Object)
	var values = make([]interface{}, 0)
	if i._SQL == "" {
		var columnNames = make([]string, 0)
		var columnValues = make([]string, 0)
		for _, field := range obj.Proto().Fields() {
			columnNames = append(columnNames, field.Name)
			columnValues = append(columnValues, "?")
		}
		i._SQL = fmt.Sprintf(insertTemplate, i.view.Table, strings.Join(columnNames, ","), strings.Join(columnValues, ","))
	}
	for _, field := range obj.Proto().Fields() {
		values = append(values, obj.ValueAt(field.Index))
	}
	if i.Query == nil {
		i.Query = metric.NewQuery(&dsc.ParametrizedSQL{SQL: i._SQL, Values: []interface{}{values}})
	} else {
		i.Query.Values = append(i.Query.Values, []interface{}{values})
	}
	i.Query.Count++
	return &dsc.ParametrizedSQL{SQL: i._SQL, Values: values}
}

//NewInsert creates a new insert
func NewInsert(view *data.View) *Insert {
	return &Insert{
		view: view,
	}
}
