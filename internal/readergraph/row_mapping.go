package readergraph

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	sqlxio "github.com/viant/sqlx/io"
	xshape "github.com/viant/x/shape"
)

// Projection names may be native SQLX aliases rather than Go identifiers.
// Only the actual row declaration grants field authority; document tags do not.
func resolveRowColumn(shape *xshape.Type, column *spec.Column) (*xshape.Resolution, error) {
	fields, err := shape.Fields()
	if err != nil {
		return nil, err
	}
	var selected *xshape.Field
	var selectedSource, selectedPhysicalSource string
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
		selectedSource = sqlTag.Name()
		selectedPhysicalSource = metadata.Source
	}
	if selected == nil {
		return nil, fmt.Errorf("reader metadata column %s has no declared Go field or native alias", column.Name)
	}
	// Bootstrap metadata may retain the SQLX result name while fresh discovery
	// reports the declared physical source. Both belong to this selected field.
	if column.Source != "" && (selectedSource != "" || selectedPhysicalSource != "") &&
		!strings.EqualFold(column.Source, selectedSource) && !strings.EqualFold(column.Source, selectedPhysicalSource) {
		return nil, fmt.Errorf("reader metadata column %s source %s differs from native sources %s and %s", column.Name, column.Source, selectedSource, selectedPhysicalSource)
	}
	return shape.ResolveField(selected.Name)
}
