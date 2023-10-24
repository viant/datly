package shared

import (
	"github.com/viant/toolbox"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func ExpandSQL(SQL string, args []interface{}) string {
	for _, arg := range args {
		if reflect.TypeOf(arg).Kind() == reflect.Ptr {
			if arg != nil {
				arg = reflect.ValueOf(arg).Elem().Interface()
			}
		}

		if arg == nil {
			SQL = strings.Replace(SQL, "?", " NULL ", 1)
			continue
		}
		switch actual := arg.(type) {
		case int:
			SQL = strings.Replace(SQL, "?", strconv.Itoa(actual), 1)
		case int64:
			SQL = strings.Replace(SQL, "?", strconv.Itoa(int(actual)), 1)
		case float64:
			SQL = strings.Replace(SQL, "?", strconv.FormatFloat(actual, 'f', 5, 32), 1)
		case bool:
			SQL = strings.Replace(SQL, "?", strconv.FormatBool(actual), 1)
		case time.Time:
			SQL = strings.Replace(SQL, "?", actual.Format(time.DateTime), 1)
		case *time.Time:
			if actual == nil {
				SQL = strings.Replace(SQL, "?", " NULL ", 1)
			} else {
				SQL = strings.Replace(SQL, "?", actual.Format(time.DateTime), 1)
			}
		default:
			val := toolbox.AsString(arg)
			SQL = strings.Replace(SQL, "?", `'`+val+`'`, 1)
		}
	}
	return SQL
}
