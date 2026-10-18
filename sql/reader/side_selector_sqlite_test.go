package reader_test

import (
	"context"
	"github.com/stretchr/testify/require"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/selectorside"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestFiveSideSelectorsSQLite(t *testing.T) {
	_, linkedMethod := reflect.TypeFor[fixture.PageComponent]().MethodByName("DatlyQuerySelectors")
	require.True(t, linkedMethod)
	for _, mode := range []string{"limited", "unlimited", "page-denied"} {
		unlimited := mode != "limited"
		t.Run(mode, func(t *testing.T) {
			found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Component]().PkgPath()}, nil)
			require.NoError(t, err)
			h := sqlite.New(t)
			require.NoError(t, h.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER,label TEXT)", "INSERT INTO records VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d')"))
			for _, source := range found.Components {

				c, err := source.Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
				require.NoError(t, err)
				if unlimited {
					if c.RootView.Selector == nil {
						c.RootView.Selector = &spec.Selector{}
					}
					c.RootView.Selector.NoLimit = true
					if mode == "page-denied" {
						require.NoError(t, c.RootView.Selector.SetPermission(spec.SelectorPropertyPage, false))
					}
					c.RootView.Source.Controls = nil
				}
				// Public and side declarations need not be grouped by the author.
				if len(c.Parameters) > 3 {
					c.Parameters[0], c.Parameters[2] = c.Parameters[2], c.Parameters[0]
				}
				artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[fixture.Output]()})
				require.NoError(t, err)
				execution, err := reader.NewExecution(reader.Config{Component: artifact.Component, InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[fixture.Output](), Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: h.DB}})
				require.NoError(t, err)
				app, err := runtime.NewRuntime([]*runtime.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[fixture.Output](), Reader: execution}})
				require.NoError(t, err)
				path := c.Routes[0].Path
				if mode == "page-denied" {
					scope, err := requestprovider.New(httptest.NewRequest("GET", path+"?page=2", nil))
					require.NoError(t, err)
					_, err = app.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: c.Key, Route: spec.RouteRef{Method: "GET", Path: path}}, Providers: scope.Providers()})
					require.ErrorContains(t, err, "selector page is not allowed")
					scope.Close()
					require.NoError(t, app.Shutdown(context.Background()))
					continue
				}
				cases := []struct {
					query string
					ids   []int
				}{{"?page=2", []int{3, 4}}, {"?page=1", []int{1, 2}}, {"", []int{1, 2}}}
				if unlimited {
					cases = []struct {
						query string
						ids   []int
					}{{"?page=bad", []int{1, 2, 3, 4}}, {"?page=2", []int{1, 2, 3, 4}}}
				}
				if source.Tag.Name == "ReadRows" {
					if unlimited {
						cases = append(cases, struct {
							query string
							ids   []int
						}{"?limit=1&page=2", []int{2}})
					} else {
						cases = append(cases, struct {
							query string
							ids   []int
						}{"?sort=id%20DESC&sort=label%20ASC&limit=1&page=2", []int{3}}, struct {
							query string
							ids   []int
						}{"?sort=id%20DESC&limit=2&offset=1&page=2", []int{2, 1}})
					}
				}
				for _, tc := range cases {
					scope, err := requestprovider.New(httptest.NewRequest("GET", path+tc.query, nil))
					require.NoError(t, err)
					value, err := app.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: c.Key, Route: spec.RouteRef{Method: "GET", Path: path}}, Providers: scope.Providers()})
					require.NoError(t, err, path+tc.query)
					var ids []int
					for _, r := range value.(*fixture.Output).Data {
						ids = append(ids, r.ID)
					}
					require.Equal(t, tc.ids, ids, path+tc.query)
					scope.Close()
				}
				require.NoError(t, app.Shutdown(context.Background()))
			}
		})
	}
}

func TestSideSelectorsTrustedRequiredInputSeedSQLite(t *testing.T) {
	found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Component]().PkgPath()}, nil)
	require.NoError(t, err)
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER,label TEXT)", "INSERT INTO records VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d')"))
	for _, source := range found.Components {
		c, err := source.Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
		require.NoError(t, err)
		for _, p := range c.Parameters {
			if p.Name == "ID" {
				required := true
				p.Required = &required
			}
		}
		artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[fixture.Output]()})
		require.NoError(t, err)
		execution, err := reader.NewExecution(reader.Config{Component: artifact.Component, InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[fixture.Output](), Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: h.DB}})
		require.NoError(t, err)
		app, err := runtime.NewRuntime([]*runtime.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[fixture.Output](), Reader: execution}})
		require.NoError(t, err)
		target := dexec.ComponentTarget{Component: c.Key, Route: spec.RouteRef{Method: "GET", Path: c.Routes[0].Path}}
		_, err = app.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: target})
		require.Error(t, err, "required public input was silently optionalized")
		value, err := app.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: target, ExtraInput: dexec.WithInput(&fixture.Input{ID: 2, Has: &fixture.InputHas{ID: true}})})
		require.NoError(t, err)
		rows := value.(*fixture.Output).Data
		require.Len(t, rows, 2)
		require.Equal(t, 3, rows[0].ID)
		require.Equal(t, 4, rows[1].ID)
		require.NoError(t, app.Shutdown(context.Background()))
	}
}
