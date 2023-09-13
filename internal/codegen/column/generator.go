package column

import (
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/discover"
	"github.com/viant/sqlx/io"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

func Generate(columns discover.Columns) (map[string]string, error) {
	result := map[string]string{}
	for viewName, viewColumns := range columns.Items {
		var columnsCode []string
		for _, column := range viewColumns {
			columnBuilder := &strings.Builder{}
			optionsBuilder := &strings.Builder{}
			if column.Tag != "" {
				optionsBuilder.WriteString(fmt.Sprintf("view.WithColumnTag(`%s`)", column.Tag))
			}
			if column.Format != "" {
				//TODO add format
			}
			optionsSeparator := ""
			if optionsBuilder.Len() > 0 {
				optionsSeparator = ","
			}
			columnBuilder.WriteString(fmt.Sprintf(`view.NewColumn("%s", "%s", %s, %v%s%s)`, column.DatabaseColumn, column.DataType, getType(column), column.Nullable, optionsSeparator, optionsBuilder.String()))
			columnsCode = append(columnsCode, columnBuilder.String())
		}
		result[viewName] = strings.Join(columnsCode, ",\n\t")
	}

	return result, nil
}

func getType(column *view.Column) string {
	dType := column.DataType
	ret, _ := io.ParseType(dType)
	if ret == nil {
		ret = reflect.TypeOf("")
	}

	switch ret.Kind() {
	case reflect.Int, reflect.Int32, reflect.Float64, reflect.Float32, reflect.String, reflect.Bool:
		if column.Nullable {
			return fmt.Sprintf("xreflect.%sPtrType", strings.Title(ret.Kind().String()))
		}
		return fmt.Sprintf("xreflect.%sType", strings.Title(ret.Kind().String()))
	}

	if xreflect.TimeType == ret {
		if column.Nullable {
			return "xreflect.TimePtrType"
		}
		return "xreflect.TimeType"

	}
	return "xreflect.StringType"
}
