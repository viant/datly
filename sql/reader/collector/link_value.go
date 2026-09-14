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
	values := r.values[link.Column]
	valueType := r.types[link.Column]
	if values == nil || valueType == nil || position < 0 || position >= len(*values) {
		return nil, fmt.Errorf("relation column %s has no value at row %d", link.Column, position)
	}
	return sqlxio.NormalizeKey(valueType.Deref((*values)[position])), nil
}
