package criteria

import (
	"fmt"
	"strings"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
)

// Validate accepts one qualified SQL identifier, not an expression or statement.
// Identifier decoding validates quoting without constructing a predicate AST.
func (m *Method) Validate() error {
	if m == nil {
		return fmt.Errorf("criteria method is required")
	}
	name := strings.TrimSpace(m.Name)
	if !methodIdentifierSpelling(name) {
		return fmt.Errorf("invalid criteria method name %q", name)
	}
	if _, err := sqlparser.TableIdentifierParts(name); err != nil {
		return fmt.Errorf("invalid criteria method name: %w", err)
	}
	m.Name = name
	return nil
}

// Keep configured names in the same spelling grammar consumed by criteria
// calls. Backtick-delimited identities may contain spaces; bare names may not.
func methodIdentifierSpelling(name string) bool {
	quoted := false
	for i := 0; i < len(name); i++ {
		ch := name[i]
		if ch == '`' {
			if quoted && i+1 < len(name) && name[i+1] == '`' {
				i++
				continue
			}
			quoted = !quoted
			continue
		}
		if !quoted && (ch <= 32 || ch >= 127 || ch == '\'' || ch == '"' || ch == '[' || ch == ']') {
			return false
		}
	}
	return !quoted
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
