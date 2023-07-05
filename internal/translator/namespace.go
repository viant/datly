package translator

import (
	"fmt"
	"github.com/viant/sqlparser"
)

type (
	Namespaces map[string]Namespace

	Namespace struct {
		Name       string
		Exclude    []string
		Transforms map[string]*Function
		View       *View
	}

	Function struct {
		Name string
		Args []string
	}
)

func NewNamespaces(SQL string) (Namespaces, error) {
	query, err := sqlparser.ParseQuery(SQL)
	if err != nil {

	}

	fmt.Printf("%v\n", query)

	return nil, nil
}
