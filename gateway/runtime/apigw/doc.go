// Package apigw defines apigw lambda datly runtime
package apigw

import (
	"fmt"
)

func f() error {

	var in interface{} = 123
	fromJSONVal, ok := in.(float64)
	if !ok {
		return fmt.Errorf("expected %T , but hS: %T", fromJSONVal, in)
	}
	i := int(fromJSONVal)
	fmt.Printf("%v\n", i)
	return nil
}
