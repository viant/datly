package datlylink

import (
	"example.com/buildapp/records"
	"github.com/viant/datly/bootstrap"
)

func init() {
	bootstrap.UseDefaultImports(records.Component{})
}
