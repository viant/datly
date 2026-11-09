package selectorcompanion_test

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	request "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/companionrootalias"
	compiler "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/builder"
	xstate "github.com/viant/xdatly/state"
	"net/http/httptest"
	"reflect"
	"strings"
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

// Expected ordering and rejection follow the original final Session.SetViewState
// result, including primary presence precedence and repeated empty items.
func TestRootOrderByLegacySessionParity(t *testing.T) {
	reflected, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Component]().PkgPath()}, nil)
	require.NoError(t, err)
	component, err := reflected.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	require.NoError(t, err)
	compiled, err := compiler.New(compiler.Input{Component: component, InputType: reflect.TypeFor[fixture.Input]()}).Compile()
	require.NoError(t, err)
	for _, tc := range []struct {
		name, query string
		order       []string
		sql         string
		invalid     bool
	}{
		{"repeated primary", "sort=NAME%20DESC&sort=ID%20ASC", []string{"NAME DESC", "ID ASC"}, "SELECT ID,NAME FROM records ORDER BY NAME DESC, ID ASC", false},
		{"repeated alias", "_orderby=NAME%20DESC&_orderby=ID%20ASC", []string{"NAME DESC", "ID ASC"}, "SELECT ID,NAME FROM records ORDER BY NAME DESC, ID ASC", false},
		{"CSV and repeated occurrence", "sort=NAME%20DESC,ID%20ASC&sort=NAME", []string{"NAME DESC", "ID ASC", "NAME"}, "SELECT ID,NAME FROM records ORDER BY NAME DESC, ID ASC, NAME", false},
		{"empty primary blocks alias", "sort=&_orderby=ID%20DESC", []string{}, "SELECT ID,NAME FROM records", false},
		{"primary first wins", "sort=NAME&_orderby=ID%20DESC", []string{"NAME"}, "SELECT ID,NAME FROM records ORDER BY NAME", false},
		{"alias first loses", "_orderby=ID%20DESC&sort=NAME", []string{"NAME"}, "SELECT ID,NAME FROM records ORDER BY NAME", false},
		{"repeated leading empty", "sort=&sort=ID%20DESC&_orderby=NAME", []string{"", "ID DESC"}, "", true},
		{"repeated trailing empty", "sort=ID&sort=&_orderby=NAME", []string{"ID", ""}, "", true},
		{"invalid primary does not fall back", "sort=BAD&_orderby=ID", []string{"BAD"}, "", true},
		{"invalid alias ignored", "sort=ID&_orderby=BAD", []string{"ID"}, "SELECT ID,NAME FROM records ORDER BY ID", false},
		{"invalid repeated item", "sort=ID&sort=BAD", []string{"ID", "BAD"}, "", true},
		{"repeated empty alias", "_orderby=&_orderby=ID%20DESC", []string{"", "ID DESC"}, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scope, err := request.New(httptest.NewRequest("GET", "/alias-records?"+tc.query, nil), request.WithIgnoreEmptyQueryParameters(false))
			require.NoError(t, err)
			defer scope.Close()
			injector, err := bindly.NewInjector(bindly.WithProviders(scope.Providers()...))
			require.NoError(t, err)
			bound, err := compiled.SelectorPlan.Bind(t.Context(), injector, &fixture.Input{}, scope.Providers()...)
			require.NoError(t, err)
			order := reflect.ValueOf(bound).Elem().FieldByName("OrderBy").Interface().([]string)
			require.Equal(t, tc.order, order)
			query, err := builder.NewBuilder().Build(t.Context(), builder.WithBuilderSQL("SELECT ID,NAME FROM records"), builder.WithBuilderSelector(&xstate.Selector{OrderBy: strings.Join(order, ",")}), builder.WithBuilderSelectorPolicy(&spec.Selector{AllowOrderBy: true}))
			if tc.invalid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.sql, query.SQL)
		})
	}
}
