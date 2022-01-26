package reader

import (
	"github.com/viant/datly/v1/data"
	"github.com/viant/sqlx/io"
	"reflect"
	"strings"
)

//DataColumnsToNames returns columns names
func DataColumnsToNames(columns []*data.Column) []string {
	result := make([]string, len(columns))
	for i := 0; i < len(columns); i++ {
		result[i] = columns[i].Name
	}

	return result
}

//TypeOf creates reflect.Type from columns types.
func TypeOf(columnTypes []io.Column) reflect.Type {
	structFields := make([]reflect.StructField, len(columnTypes))
	for i := 0; i < len(columnTypes); i++ {
		scanType := columnTypes[i].ScanType()
		structFields[i] = reflect.StructField{Name: strings.Title(columnTypes[i].Name()), Type: scanType}
	}

	return reflect.StructOf(structFields)
}
