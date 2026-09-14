package criteria

import (
	"fmt"
	"strings"

	"github.com/viant/parsly"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
)

// Validate canonicalizes an authored function name through the SQL parser.
// Only one function identity is accepted; argument or statement text cannot
// become part of the configured SQL emitted for a client method call.
func (m *Method) Validate() error {
	if m == nil {
		return fmt.Errorf("criteria method is required")
	}
	cursor := parsly.NewCursor("criteria method", []byte("criteria_value = "+strings.TrimSpace(m.Name)+"() "), 0)
	qualified := &expr.Qualify{}
	if err := sqlparser.ParseQualify(cursor, qualified); err != nil {
		return fmt.Errorf("invalid criteria method name: %w", err)
	}
	if strings.TrimSpace(string(cursor.Input[cursor.Pos:])) != "" {
		return fmt.Errorf("invalid criteria method name %q", m.Name)
	}
	binary, ok := qualified.X.(*expr.Binary)
	if !ok || binary.Op != "=" {
		return fmt.Errorf("invalid criteria method name %q", m.Name)
	}
	call, ok := binary.Y.(*expr.Call)
	if !ok || len(call.Args) != 0 {
		return fmt.Errorf("invalid criteria method name %q", m.Name)
	}
	switch call.X.(type) {
	case *expr.Ident, *expr.Selector:
	default:
		return fmt.Errorf("invalid criteria method identity")
	}
	m.Name = sqlparser.Stringify(call.X)
	return nil
}

func (c *compilation) method(call *expr.Call) (string, error) {
	name := sqlparser.Stringify(call.X)
	var method *Method
	for key, candidate := range c.compiler.Methods {
		if strings.EqualFold(key, name) {
			copy := candidate
			method = &copy
			break
		}
	}
	if method == nil {
		return "", fmt.Errorf("criteria method %q is not configured", name)
	}
	if err := method.Validate(); err != nil {
		return "", err
	}
	if len(call.Args) != len(method.Args) {
		return "", fmt.Errorf("criteria method %q expects %d arguments", name, len(method.Args))
	}
	args := make([]string, len(call.Args))
	for i, arg := range call.Args {
		var err error
		args[i], err = c.value(arg, Column{Type: method.Args[i]})
		if err != nil {
			return "", err
		}
	}
	return method.Name + "(" + strings.Join(args, ", ") + ")", nil
}
