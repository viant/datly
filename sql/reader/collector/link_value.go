package collector

import (
	"fmt"

	sqlxio "github.com/viant/sqlx/io"
	"github.com/viant/xunsafe"
)

// linkKeyAt reads a relation key from the typed row when mapped, otherwise
// from SQLx's unmapped-column destination for the same row position.
func (r *Collector) linkKeyAt(row any, link *Link, position int) (any, error) {
	if link == nil {
		return nil, fmt.Errorf("relation link is required")
	}
	if link.XField != nil {
		return sqlxio.NormalizeKey(link.XField.Interface(xunsafe.AsPointer(row))), nil
	}
	value, ok := r.sqlKeyAt(link.Column, position)
	if !ok {
		return nil, fmt.Errorf("relation column %s has no value at row %d", link.Column, position)
	}
	return value, nil
}

func (r *Collector) sqlKeyAt(column string, position int) (any, bool) {
	values := r.values[column]
	if values == nil || position < 0 || position >= len(*values) || (*values)[position] == nil {
		return nil, false
	}
	if snapshot, ok := (*values)[position].(*any); ok {
		return sqlxio.NormalizeKey(*snapshot), true
	}
	valueType := r.types[column]
	if valueType == nil {
		return nil, false
	}
	return sqlxio.NormalizeKey(valueType.Deref((*values)[position])), true
}
