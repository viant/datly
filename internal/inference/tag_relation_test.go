package inference

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view"
	vstate "github.com/viant/datly/view/state"
	"github.com/viant/datly/view/tags"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/query"
)

func TestTags_buildRelation_UsesColumnAliasWhenNameIsEmpty(t *testing.T) {
	selectQuery, err := sqlparser.ParseQuery("SELECT 'app' AS SITE_TYPE_VALUE")
	require.NoError(t, err)

	parentColumn := &sqlparser.Column{Name: "SITE_TYPE_VALUES"}
	childColumn := &sqlparser.Column{Alias: "SITE_TYPE_VALUE"}

	parentField := &Field{
		Field:  view.Field{Name: "SiteTypeValues", Schema: &vstate.Schema{}},
		Column: parentColumn,
	}
	keyField := &Field{
		Field:  view.Field{Name: "SiteTypeValue", Schema: &vstate.Schema{}},
		Column: childColumn,
	}

	spec := &Spec{Table: "ignored"}
	relation := &Relation{
		Name:        "siteType",
		Join:        &query.Join{Alias: "siteType", With: selectQuery},
		ParentField: parentField,
		KeyField:    keyField,
		Pairs: []*RelationPair{{
			ParentField: parentField,
			KeyField:    keyField,
		}},
	}

	field := &Field{Field: view.Field{Name: "SiteType", Schema: &vstate.Schema{}}, Tags: Tags{}}
	field.Tags.buildRelation(spec, relation)
	tagString := field.Tags.Stringify()

	parsed, err := tags.Parse(reflect.StructTag(tagString), nil, tags.LinkOnTag)
	require.NoError(t, err)
	require.Len(t, parsed.LinkOn, 1)

	var relField, relColumn, refField, refColumn string
	require.NoError(t, parsed.LinkOn.ForEach(func(rf, rc, kf, kc string, include *bool) error {
		relField, relColumn, refField, refColumn = rf, rc, kf, kc
		return nil
	}))

	require.Equal(t, "SiteTypeValues", relField)
	require.Equal(t, "SITE_TYPE_VALUES", relColumn)
	require.Equal(t, "SiteTypeValue", refField)
	require.Equal(t, "SITE_TYPE_VALUE", refColumn)
}

func TestType_ByColumn_MatchesAlias(t *testing.T) {
	typ := &Type{columnFields: []*Field{{
		Field:  view.Field{Name: "SiteTypeValue", Schema: &vstate.Schema{}},
		Column: &sqlparser.Column{Alias: "SITE_TYPE_VALUE"},
	}}}
	require.NotNil(t, typ.ByColumn("SITE_TYPE_VALUE"))
}
