package translator

import (
	"encoding/json"
	"fmt"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/query"
	"strings"
)

type Namespaces map[string]*Namespace

func (n Namespaces) Init(query *query.Select) error {
	namespace := NewNamespace(query.From.Alias, sqlparser.Stringify(query.From.X))
	n[namespace.Name] = namespace
	for i := range query.Joins {
		join := query.Joins[i]
		relNamespace := NewNamespace(join.Alias, sqlparser.Stringify(join.With))
		n[relNamespace.Name] = relNamespace
	}
	err := n.updateSettings(query, namespace)
	if err != nil {
		return err
	}
	return nil
}

func (n Namespaces) updateSettings(query *query.Select, namespace *Namespace) error {
	columns := sqlparser.NewColumns(query.List)
	for i := range columns {
		if err := n.updateSetting(columns[i], namespace); err != nil {
			return err
		}
	}
	return nil
}

func (n Namespaces) updateSetting(column *sqlparser.Column, namespace *Namespace) error {
	if column.Namespace == "" {
		column.Namespace = namespace.Name
	}
	namespaceForColumn, ok := n[column.Namespace]
	if !ok {
		return fmt.Errorf("unknown query namespace: %v", column.Namespace)
	}
	if len(column.Except) > 0 {
		namespaceForColumn.Exclude = column.Except
	}
	if column.Comments != "" && strings.HasSuffix(column.Expression, "*") {
		if err := json.Unmarshal([]byte(column.Comments), &namespaceForColumn.OutputConfig); err != nil {
			return fmt.Errorf("invalid hint for %v, %w", namespace.Name, err)
		}
	}
	return nil
}
