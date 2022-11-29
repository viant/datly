package expand

import (
	"fmt"
	"github.com/viant/velty/ast/expr"
)

func unexpectedArgType(position int, expected interface{}, got interface{}) error {
	return fmt.Errorf("unexpected arg[%v] type, expected %T, got %T", position, expected, got)
}

func checkArgsSize(call *expr.Call, size int) error {
	if len(call.Args) != size {
		return fmt.Errorf("unexpected number of function %v arguments, expected 1 got %v", queryFunctionName, len(call.Args))
	}
	return nil
}
