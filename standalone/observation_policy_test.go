package standalone

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/observability"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/records"
)

func TestStandaloneObservationPolicySourceAndLinkedSQLite(t *testing.T) {
	for _, linked := range []bool{false, true} {
		t.Run(map[bool]string{false: "source", true: "linked"}[linked], func(t *testing.T) {
			ctx := context.Background()
			f := fixture.New(t)
			discovered, err := bootstrap.ReflectSelectedPackages([]string{fixture.Module + "/records"}, nil)
			require.NoError(t, err)
			var identity string
			for _, linkedComponent := range discovered.Components {
				component, err := linkedComponent.Resolve(linkedComponent.LinkedInputType, linkedComponent.LinkedOutputType)
				require.NoError(t, err)
				if component.Key.Name == "Read" {
					identity, err = component.RootView.Identity()
					require.NoError(t, err)
				}
			}
			require.NotEmpty(t, identity)
			p := &observability.Policy{Views: []observability.ViewObservation{{Component: spec.Key{Kind: spec.KindComponent, Scope: fixture.Module + "/records", Name: "Read"}, ViewIdentity: identity, Diagnostic: "records#", Operation: &observability.OperationDescriptor{Name: "platform.records", Location: "platform", Description: "records performance", Provider: observability.Source11}}}}
			f.WriteConfig(t, func(c map[string]any) {
				c["GoBootstrap"] = map[string]any{"Packages": []string{fixture.Module + "/records"}, "LinkedOnly": linked}
				c["Meta"] = map[string]any{"MetricURI": "/metric"}
				c["Observation"] = map[string]any{"Policy": p}
			})
			cfg, err := (config.Loader{}).Load(ctx, f.Config)
			require.NoError(t, err)
			exports, err := records.Exports()
			require.NoError(t, err)
			server, err := New(ctx, Options{Config: cfg, Registry: exports})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, server.Shutdown(ctx)) })
			// Mutation after owner construction must not alter lazy materialization.
			cfg.Observation.Policy.Views[0].Operation.Name = "caller mutation"
			cfg.Observation.Policy.Views[0].Diagnostic = "caller mutation"
			require.NoError(t, server.Reload(ctx, 1))
			request := func(path string) *httptest.ResponseRecorder {
				w := httptest.NewRecorder()
				server.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
				return w
			}
			before := request("/metric/operations")
			require.Equal(t, 200, before.Code, before.Body.String())
			require.JSONEq(t, "null", before.Body.String())
			w := request("/records/1")
			if w.Code != 200 {
				_, directErr := server.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: fixture.Module + "/records", Name: "Read"}, Route: spec.RouteRef{Method: "GET", Path: "/records/{id}"}}, Input: &records.ReadInput{ID: 1}})
				require.NoError(t, directErr, "unmasked native failure")
			}
			require.Equal(t, 200, w.Code, w.Body.String())
			require.JSONEq(t, `{"rows":[{"id":1,"name":"first"}]}`, w.Body.String())
			require.Empty(t, w.Header().Get("Datly-Metrics"))
			var operations []struct {
				Name     string
				Count    int64
				Counters []struct {
					Value string
					Count int64
				}
			}
			catalog := request("/metric/operations")
			require.Equal(t, 200, catalog.Code)
			require.NoError(t, json.Unmarshal(catalog.Body.Bytes(), &operations))
			require.Len(t, operations, 1)
			require.Equal(t, "platform.records", operations[0].Name)
			require.EqualValues(t, 1, operations[0].Count)
			require.Len(t, operations[0].Counters, 11)
			require.Equal(t, "Success", operations[0].Counters[0].Value)
			require.EqualValues(t, 1, operations[0].Counters[0].Count)
			require.Zero(t, operations[0].Counters[2].Count)
		})
	}
}
