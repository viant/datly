package writer

import (
	"fmt"
	"strings"

	"github.com/viant/datly/constant"
	"github.com/viant/datly/spec"
	sqltemplate "github.com/viant/datly/sql/template"
	"github.com/viant/sqlparser"
)

func resolvedWriterTable(component *spec.Component, table string) (string, error) {
	if !strings.Contains(table, "$") {
		return table, nil
	}
	// Bootstrap has applied the immutable instance to the component's declared
	// constants. Use the reader's identifier renderer for DML too; invocation
	// values never select a writer's physical target.
	var defaults *constant.Values
	values, err := defaults.For(component)
	if err != nil {
		return "", err
	}
	table, err = (sqltemplate.ConstantRenderer{Values: values}).Identifier(table)
	if err != nil {
		return "", err
	}
	if strings.Contains(table, "$") {
		return "", fmt.Errorf("unresolved table identifier")
	}
	if parts, parseErr := sqlparser.TableIdentifierParts(table); parseErr == nil && len(parts) == 1 {
		table = parts[0]
	}
	return table, nil
}
