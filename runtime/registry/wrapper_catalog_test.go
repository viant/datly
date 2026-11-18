package registry

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

type wrapperCatalogBody struct {
	Body []int `jsonx:"wrapper=ids"`
}
type wrapperCatalogPlain struct{ Body []int }

func TestWrapperCatalogPolicyIdentity(t *testing.T) {
	for _, tc := range []struct {
		name          string
		first, second reflect.Type
		a, b          reflect.StructTag
		conflict      bool
	}{
		{"inferred matches explicit", reflect.TypeFor[wrapperCatalogBody](), reflect.TypeFor[wrapperCatalogPlain](), "", `jsonx:"wrapper=ids"`, false},
		{"strict differs", reflect.TypeFor[wrapperCatalogBody](), reflect.TypeFor[wrapperCatalogPlain](), "", "", true},
		{"different members", reflect.TypeFor[wrapperCatalogPlain](), reflect.TypeFor[wrapperCatalogPlain](), `jsonx:"wrapper=ids"`, `jsonx:"wrapper=values"`, true},
		{"equivalent order", reflect.TypeFor[wrapperCatalogPlain](), reflect.TypeFor[wrapperCatalogPlain](), `jsonx:"wrapper=ids,null=omit"`, `jsonx:" null=omit, wrapper=ids "`, false},
		{"different null policy", reflect.TypeFor[wrapperCatalogPlain](), reflect.TypeFor[wrapperCatalogPlain](), `jsonx:"wrapper=ids,null=omit"`, `jsonx:"wrapper=ids"`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := inputProjection{root: spec.RouteRef{Method: "POST", Path: "/parent"}, locations: map[string]int{}}
			a := catalogCollectionField(t, tc.first, tc.a, "/child")
			b := catalogCollectionField(t, tc.second, tc.b, "/parent")
			require.NoError(t, p.add(a))
			err := p.add(b)
			require.Equal(t, tc.conflict, err != nil, "%v", err)
		})
	}
}

func TestWrapperInputDomainMetadata(t *testing.T) {
	field, _ := reflect.TypeFor[wrapperCatalogBody]().FieldByName("Body")
	schema, err := JSONXInputSchema(field)
	require.NoError(t, err)
	require.NotNil(t, schema)
	require.Equal(t, "ids", schema.Wrapper)
	require.Equal(t, schema, schema.Clone())
	require.Len(t, schema.AnyOf, 2)
	require.Equal(t, "array", schema.AnyOf[0].Type)
	require.Equal(t, "object", schema.AnyOf[1].Type)
}
