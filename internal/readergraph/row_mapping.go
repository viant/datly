package readergraph

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	sqlxio "github.com/viant/sqlx/io"
	xshape "github.com/viant/x/shape"
)

// ResolveRowColumn matches projection names through native SQLX aliases or Go identifiers.
// Only the actual row declaration grants field authority; document tags do not.
func ResolveRowColumn(shape *xshape.Type, column *spec.Column) (*xshape.Resolution, error) {
	fields, err := shape.Fields()
	if err != nil {
		return nil, err
	}
	var selected *xshape.Field
	for i := range fields {
		field := &fields[i]
		sqlTag := sqlxio.ParseTag(field.Tag)
		if !field.Exported || sqlTag.Transient || sqlTag.PresenceProvider || field.Tag.Get("setMarker") == "true" {
			continue
		}
		metadata, err := dtag.ParseField(field.StructField())
		if err != nil {
			return nil, err
		}
		matches := field.Name == column.Name
		for _, alias := range strings.Split(sqlTag.Column, "|") {
			alias = strings.TrimSpace(alias)
			if alias != "" && strings.EqualFold(alias, column.Name) {
				matches = true
			}
		}
		if metadata.SelectorAlias != "" && metadata.SelectorAlias == column.Name {
			matches = true
		}
		if !matches {
			continue
		}
		if selected != nil {
			return nil, fmt.Errorf("reader metadata column %s has ambiguous native field mapping", column.Name)
		}
		selected = field
	}
	if selected == nil {
		return nil, fmt.Errorf("reader metadata column %s has no declared Go field or native alias", column.Name)
	}
	// Source describes SQL lineage, not Go field identity. Existing producers
	// may retain either physical or result names; row validation checks the
	// selected declaration and its canonical type without rewriting that lineage.
	return shape.ResolveField(selected.Name)
}
