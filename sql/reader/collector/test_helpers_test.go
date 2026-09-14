package collector

import (
	"reflect"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"github.com/viant/xunsafe"
)

func newTestView(metadata *data.View, rowType reflect.Type) *View {
	if metadata == nil {
		metadata = &data.View{}
	}
	return &View{View: metadata, Schema: NewSchema(rowType)}
}

func newTestLink(namespace, column, field string, xfield *xunsafe.Field) *Link {
	return &Link{Link: &data.Link{Namespace: namespace, Column: column, Field: field}, XField: xfield}
}

func newTestRelation(parent, child *View, name, holder string, cardinality spec.Cardinality, parentLinks, childLinks Links) *Relation {
	metadata := &data.Relation{
		Name: name, Holder: holder, Cardinality: cardinality,
		On: metadataLinks(parentLinks),
		Of: &data.RelationRef{View: child.View, On: metadataLinks(childLinks), MatchStrategy: data.MatchSequential},
	}
	result := &Relation{
		Relation: metadata,
		On:       parentLinks,
		Of:       &RelationRef{RelationRef: metadata.Of, View: child, On: childLinks},
	}
	if holder != "" && parent != nil && parent.Schema.RowType() != nil {
		result.HolderField = xunsafe.FieldByName(parent.Schema.RowType(), holder)
		if result.HolderField != nil && result.HolderField.Type.Kind() == reflect.Slice {
			result.HolderSlice = xunsafe.NewSlice(result.HolderField.Type)
		}
	}
	return result
}

func metadataLinks(links Links) data.Links {
	result := make(data.Links, 0, len(links))
	for _, link := range links {
		if link != nil {
			result = append(result, link.Link)
		}
	}
	return result
}
