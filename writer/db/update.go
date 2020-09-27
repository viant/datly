package db

import (
	"fmt"
	"github.com/viant/datly/data"
	"github.com/viant/gtly"
	"github.com/viant/datly/metric"
	"github.com/viant/dsc"
	"strings"
)

const updateTemplate = "UPDATE %v SET %v WHERE %v"

type Update struct {
	Queries []*metric.Query
	view    *data.View
	pk      map[string]bool
}


func (u *Update) DML(item interface{}) *dsc.ParametrizedSQL {
	obj := item.(*gtly.Object)
	var values = make([]interface{}, 0)
	var columns = make([]string, 0)
	var whereColumns = make([]string, 0)

	for _, field := range obj.Proto().Fields() {
		if u.pk[field.Name] {
			continue
		}
		if !obj.HasAt(field.Index) {
			continue
		}
		columns = append(columns, fmt.Sprintf("%v = ?", field.Name))
		values = append(values, obj.ValueAt(field.Index))
	}
	for _, pk := range u.view.PrimaryKey {
		whereColumns = append(whereColumns, fmt.Sprintf("%v = ?", pk))
		values = append(values, obj.Value(pk))
	}
	SQL := fmt.Sprintf(updateTemplate, u.view.Table, strings.Join(columns, ",\n"), strings.Join(whereColumns, "."))
	result := &dsc.ParametrizedSQL{SQL: SQL, Values: values}
	query := metric.NewQuery(u.view.Name, result)
	query.Count++
	u.Queries = append(u.Queries, query)
	return result
}

//NewInsert creates a new insert
func NewUpdate(view *data.View) *Update {
	var pk = make(map[string]bool)
	for i := range view.PrimaryKey {
		pk[view.PrimaryKey[i]] = true
	}
	return &Update{
		view:    view,
		Queries: make([]*metric.Query, 0),
		pk:      pk,
	}
}
