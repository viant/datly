package logger

import (
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/velty/est"
	"github.com/viant/velty/est/op"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
)

var stringType = reflect.TypeOf("")

type Printer struct {
	buffer []string
}

func (p *Printer) Discover(aFunc interface{}) (func(operands []*op.Operand, state *est.State) (interface{}, error), reflect.Type, bool) {
	switch actual := aFunc.(type) {
	case func(_ *Printer, args ...interface{}) string:
		return func(operands []*op.Operand, state *est.State) (interface{}, error) {
			return actual(p, p.asInterfaces(operands[1:], state)), nil
		}, stringType, true

	case func(_ *Printer, message string, args ...interface{}) string:
		return func(operands []*op.Operand, state *est.State) (interface{}, error) {
			if len(operands) < 1 {
				return nil, fmt.Errorf("expected to get 1 or more arguments but got %v", len(operands))
			}

			format := *(*string)(operands[1].Exec(state))
			args := p.asInterfaces(operands[2:], state)

			return actual(p, format, args...), nil
		}, stringType, true

	}

	return nil, nil, false
}

func (p *Printer) asInterfaces(operands []*op.Operand, state *est.State) []interface{} {
	args := make([]interface{}, len(operands))

	for i, operand := range operands {
		value := reflect.New(operand.Type).Elem().Interface()
		xunsafe.Copy(xunsafe.AsPointer(value), operand.Exec(state), int(operand.Type.Size()))
		args[i] = value
	}

	return args
}

func (p *Printer) Println(args ...interface{}) string {
	fmt.Println(args...)
	return ""
}

func (p *Printer) Printf(format string, args ...interface{}) string {
	fmt.Printf(p.Sprintf(format, args...))
	return ""
}

func (p *Printer) Log(format string, args ...interface{}) string {
	p.buffer = append(p.buffer, p.Sprintf(format, args...))
	return ""
}

func (p *Printer) Logf(format string, args ...interface{}) string {
	return p.Log(format, args)
}

func (p *Printer) Sprintf(format string, args ...interface{}) string {
	return fmt.Sprintf(strings.ReplaceAll(format, "\\n", "\n"), args...)
}

func (p *Printer) Flush() {
	for _, s := range p.buffer {
		fmt.Print(s)
	}
}

func (p *Printer) Fatal(any interface{}, args ...interface{}) (string, error) {
	format, ok := any.(string)
	if ok {
		return "", fmt.Errorf(p.Sprintf(format, args...))
	}
	if err, ok := any.(error); ok {
		return "", err
	}
	return "", fmt.Errorf(p.Sprintf("%+v", any))
}

//Fatalf fatal with formatting
func (p *Printer) Fatalf(any interface{}, args ...interface{}) (string, error) {
	return p.Fatal(any, args...)
}

//FatalfWithCode logs and terminate with status code
func (p *Printer) FatalfWithCode(code int, any interface{}, args ...interface{}) (string, error) {
	format, ok := any.(string)
	if ok {
		err := &shared.ResponseError{StatusCode: code, Origin: fmt.Errorf(p.Sprintf(format, args...))}
		return "", err
	}
	if err, ok := any.(error); ok {
		return "", &shared.ResponseError{StatusCode: code, Origin: err}
	}
	err := &shared.ResponseError{StatusCode: code, Origin: fmt.Errorf(p.Sprintf("%+v", any))}
	return "", err
}
