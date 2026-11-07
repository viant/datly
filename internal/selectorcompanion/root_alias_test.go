package selectorcompanion_test

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	request "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/companionrootalias"
	compiler "github.com/viant/datly/runtime/handler/compiler"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestDeclaredRootListSelectorAliases(t *testing.T) {
	reflected, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Component]().PkgPath()}, nil)
	require.NoError(t, err)
	component, err := reflected.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	require.NoError(t, err)
	compiled, err := compiler.New(compiler.Input{Component: component, InputType: reflect.TypeFor[fixture.Input]()}).Compile()
	require.NoError(t, err)
	for _, tc := range []struct {
		name, query                            string
		fields, order, childFields, childOrder []string
	}{
		{"root aliases", "?_fields=id&_orderby=id%20DESC", []string{"id"}, []string{"id DESC"}, nil, nil},
		{"primary wins", "?fields=other&sort=other&_fields=id&_orderby=id", []string{"other"}, []string{"other"}, nil, nil},
		{"lists preserved", "?_fields=id,label&_fields=extra&_orderby=id&_orderby=label", []string{"id", "label", "extra"}, []string{"id", "label"}, nil, nil},
		{"children independent", "?_fields=id&_orderby=id&childFields=parent_id&childSort=parent_id", []string{"id"}, []string{"id"}, []string{"parent_id"}, []string{"parent_id"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scope, err := request.New(httptest.NewRequest("GET", "/alias-records"+tc.query, nil))
			require.NoError(t, err)
			defer scope.Close()
			injector, err := bindly.NewInjector(bindly.WithProviders(scope.Providers()...))
			require.NoError(t, err)
			bound, err := compiled.SelectorPlan.Bind(t.Context(), injector, &fixture.Input{}, scope.Providers()...)
			require.NoError(t, err)
			v := reflect.ValueOf(bound).Elem()
			require.Equal(t, tc.fields, v.FieldByName("Fields").Interface())
			require.Equal(t, tc.order, v.FieldByName("OrderBy").Interface())
			require.Equal(t, tc.childFields, v.FieldByName("ChildFields").Interface())
			require.Equal(t, tc.childOrder, v.FieldByName("ChildOrder").Interface())
		})
	}
}

func TestRootAliasRespectsNativeEmptyQueryPolicy(t *testing.T) {
	reflected, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Component]().PkgPath()}, nil)
	require.NoError(t, err)
	component, err := reflected.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	require.NoError(t, err)
	compiled, err := compiler.New(compiler.Input{Component: component, InputType: reflect.TypeFor[fixture.Input]()}).Compile()
	require.NoError(t, err)
	bind := func(query string, ignore bool) any {
		scope, err := request.New(httptest.NewRequest("GET", "/alias-records"+query, nil), request.WithIgnoreEmptyQueryParameters(ignore))
		require.NoError(t, err)
		defer scope.Close()
		injector, err := bindly.NewInjector(bindly.WithProviders(scope.Providers()...))
		require.NoError(t, err)
		bound, err := compiled.SelectorPlan.Bind(t.Context(), injector, &fixture.Input{}, scope.Providers()...)
		require.NoError(t, err)
		return reflect.ValueOf(bound).Elem().FieldByName("Fields").Interface()
	}
	require.Equal(t, bind("?fields=", false), bind("?fields=&_fields=id", false), "present empty primary must retain authority")
	require.Equal(t, []string{"id"}, bind("?fields=&_fields=id", true), "ignored empty primary permits canonical fallback")
	require.Equal(t, bind("?fields=&fields=", true), bind("?fields=&fields=&_fields=id", true), "repeated empty values retain native presence semantics")
}
