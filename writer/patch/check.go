package patch

import (
	"context"
	"fmt"
	"github.com/viant/datly/data"
	"github.com/viant/datly/metric"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"strings"
)

const existingTemplate = "SELECT %v FROM %v WHERE (%v) IN (%v)"

func (p *service) removeNonExisting(ctx context.Context, manager dsc.Manager, conn dsc.Connection, view *data.View, index map[string][]interface{}, metrics *metric.Metrics) error {
	if len(index) == 0 {
		return nil
	}
	var SQLValues = make([]interface{}, 0)
	var placeholders = make([]string, 0)
	var repeat = "?"
	if len(view.PrimaryKey) > 1 {
		repeat = strings.Repeat("?", len(view.PrimaryKey))
		repeat = "(" + (repeat[:len(repeat)-1]) + ")"
	}
	for _, v := range index {
		SQLValues = append(SQLValues, v...)
		placeholders = append(placeholders, repeat)
	}
	var result = make(map[string]bool)
	SQL := fmt.Sprintf(existingTemplate, strings.Join(view.PrimaryKey, ","), view.Table, strings.Join(view.PrimaryKey, ","), strings.Join(placeholders, ","))
	var record = make([]interface{}, len(view.PrimaryKey))
	var keys = make([]string, len(view.PrimaryKey))
	query := metric.NewQuery(view.Name, &dsc.ParametrizedSQL{SQL: SQL, Values: SQLValues})
	err := manager.ReadAllOnWithHandlerOnConnection(conn, SQL, SQLValues, func(scanner dsc.Scanner) (toContinue bool, err error) {
		query.Increment()
		var values = make([]interface{}, len(record))
		for i := range values {
			values[i] = &record[i]
		}
		err = scanner.Scan(values...)
		if err != nil {
			return false, err
		}
		for i, v := range record {
			keys[i] = toolbox.AsString(v)
		}
		result[strings.Join(keys, "-")] = true
		return true, nil
	})
	query.SetFetchTime()
	metrics.AddQuery(query)
	for k := range index {
		if _, ok := result[k]; !ok {
			delete(index, k)
		}
	}
	return err
}
