package provider

import (
	"github.com/viant/datly/view"
	"reflect"
)

type Table struct {
	View       *view.View
	SQL        string
	Relations  []*Table
	RecordType reflect.Type
}
