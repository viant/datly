package compiler

import (
	"fmt"
	"reflect"
	"strings"

	dtag "github.com/viant/datly/tag"
	"github.com/viant/datly/typecatalog"
)

type parsedLink struct {
	parentField     string
	parentNamespace string
	parentColumn    string
	childField      string
	childNamespace  string
	childColumn     string
}

func parseLinkOn(links []*dtag.RelationLink, parentType, childType reflect.Type) ([]parsedLink, error) {
	result := make([]parsedLink, 0, len(links))
	for _, link := range links {
		if link == nil {
			return nil, fmt.Errorf("relation link is required")
		}
		if link.Parent.Include != nil {
			return nil, fmt.Errorf("relation include flag is not supported by reader compilation; shape the relation key in the authored output type")
		}
		parentField, parentColumn, err := resolveLinkPart(link.Parent, parentType)
		if err != nil {
			return nil, err
		}
		childField, childColumn, err := resolveLinkPart(link.Child, childType)
		if err != nil {
			return nil, err
		}
		result = append(result, parsedLink{
			parentField: parentField, parentNamespace: link.Parent.Namespace, parentColumn: parentColumn,
			childField: childField, childNamespace: link.Child.Namespace, childColumn: childColumn,
		})
	}
	return result, nil
}

func resolveLinkPart(part dtag.RelationPart, ownerType reflect.Type) (string, string, error) {
	fieldName := strings.TrimSpace(part.Field)
	column := strings.TrimSpace(part.Column)
	if fieldName == "" {
		columns, err := columnsFromType(ownerType)
		if err != nil {
			return "", "", err
		}
		for _, candidate := range columns {
			if candidate != nil && strings.EqualFold(candidate.Column, column) {
				if fieldName != "" && fieldName != candidate.Name {
					return "", "", fmt.Errorf("column %q is ambiguous on %s", column, ownerType)
				}
				fieldName = candidate.Name
			}
		}
		if fieldName != "" {
			return fieldName, column, nil
		}
		field, ok := typecatalog.FieldByName(ownerType, column)
		if !ok {
			return "", "", fmt.Errorf("unable to resolve field for column %q on %s", column, ownerType)
		}
		fieldName = field.Name
	}
	// Explicit fields may name hidden SQLX relation keys that are not present on
	// the public row struct. Collector key resolution handles those by column.
	return fieldName, column, nil
}
