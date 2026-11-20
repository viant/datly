// Package associationtag resolves the member selected by a primitive association input.
package associationtag

import (
	"fmt"
	"go/token"
	"reflect"

	"github.com/viant/tagly/tags"
)

// Descriptor maps the public primitive collection to a field on each association row.
// The writer relation remains authoritative for the table and parent links.
type Descriptor struct {
	Collection         string
	InsertIdentityRole string
	Member             reflect.StructField
}

// Parse returns nil for a holder that does not opt in.
func Parse(holder reflect.StructField) (*Descriptor, error) {
	raw, ok := holder.Tag.Lookup("association")
	if !ok {
		return nil, nil
	}
	if holder.PkgPath != "" || holder.Type.Kind() != reflect.Slice {
		return nil, fmt.Errorf("association on %s requires an exported slice of records", holder.Name)
	}
	row := holder.Type.Elem()
	if row.Kind() == reflect.Pointer {
		row = row.Elem()
	}
	if row.Kind() != reflect.Struct {
		return nil, fmt.Errorf("association on %s requires a record element", holder.Name)
	}
	collection, member, role, err := parseNames(holder.Name, raw)
	if err != nil {
		return nil, err
	}
	field, ok := row.FieldByName(member)
	if !ok || field.PkgPath != "" || len(field.Index) != 1 {
		return nil, fmt.Errorf("association on %s requires a direct exported member %s on %s", holder.Name, member, row)
	}
	scalar := field.Type
	if scalar.Kind() == reflect.Pointer {
		scalar = scalar.Elem()
	}
	switch scalar.Kind() {
	case reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return &Descriptor{Collection: collection, Member: field, InsertIdentityRole: role}, nil
	default:
		return nil, fmt.Errorf("association member %s.%s must be a string or integer", row, member)
	}
}

// Names resolves the declaration before a generated Go type exists.
func Names(holder, raw string) (string, string, error) {
	collection, member, _, err := parseNames(holder, raw)
	return collection, member, err
}

func parseNames(holder, raw string) (string, string, string, error) {
	var collection, member, role string
	err := tags.Values(raw).MatchPairs(func(key, value string) error {
		var target *string
		switch key {
		case "collection":
			target = &collection
		case "insertIdentityRole":
			target = &role
		case "member":
			target = &member
		default:
			return fmt.Errorf("unknown association option %q on %s", key, holder)
		}
		if *target != "" || !token.IsIdentifier(value) {
			return fmt.Errorf("invalid or duplicate association %s on %s", key, holder)
		}
		*target = value
		return nil
	})
	if err != nil {
		return "", "", "", err
	}
	if collection == "" || member == "" {
		return "", "", "", fmt.Errorf("association on %s requires collection and member", holder)
	}
	return collection, member, role, nil
}
