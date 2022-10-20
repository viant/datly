package sanitize

import "reflect"

type (
	Typer interface{}

	LiteralType struct {
		RType    reflect.Type
		DataType string
	}

	ColumnType struct {
		ColumnName string
	}
)

func NewLiteralType(rType reflect.Type) *LiteralType {
	return &LiteralType{
		RType:    rType,
		DataType: rType.String(),
	}
}
