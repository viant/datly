package standalone

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/selectorpolicy"
	"github.com/viant/mcp-protocol/schema"
)

func TestStandaloneSelectorExplicitPolicy(t *testing.T) {
	for _, eager := range []bool{false, true} {
		t.Run(fmt.Sprintf("eager=%v", eager), func(t *testing.T) {
			ctx := context.Background()
			f := fixture.New(t)
			require.NoError(t, f.DB.ExecStatements(ctx, "INSERT INTO records(id,name) VALUES(2,'second')"))
			f.WriteConfig(t, func(c map[string]any) {
				c["GoBootstrap"] = map[string]any{"Packages": []string{fixture.Module + "/selectorpolicy"}, "EagerComponents": eager}
			})
			cfg, err := (config.Loader{}).Load(ctx, f.Config)
			require.NoError(t, err)
			server, err := New(ctx, Options{Config: cfg})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, server.Shutdown(context.Background())) })
			require.NoError(t, server.Reload(ctx, 1))
			native := (mcpclient.Config{Source: server.manager}).New(t)
			for _, policy := range []struct {
				path, tool string
				denied     bool
			}{
				{"/selector-denied", "Denied", true},
				{"/selector-allowed", "Allowed", false},
				{"/selector-inferred", "Inferred", false},
			} {
				for _, shape := range []struct {
					name, query string
					args        map[string]any
					id          int
				}{
					{"omitted", "", map[string]any{}, 1},
					{"sorting", "?orderBy=id%20desc", map[string]any{"orderBy": "id desc"}, 2},
					{"paging", "?page=2", map[string]any{"page": 2}, 2},
				} {
					t.Run(policy.tool+"/"+shape.name, func(t *testing.T) {
						denied := policy.denied && shape.name != "omitted"
						input := &selectorpolicy.Input{}
						if shape.name == "sorting" {
							input.OrderBy = "id desc"
						}
						if shape.name == "paging" {
							input.Page = 2
						}
						_, invocationErr := server.InvokeComponent(ctx, exec.ComponentRequest{Target: exec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: fixture.Module + "/selectorpolicy", Name: policy.tool}, Route: spec.RouteRef{Method: "GET", Path: policy.path}}, Input: input})
						if denied {
							require.ErrorContains(t, invocationErr, "not allowed")
						} else {
							require.NoError(t, invocationErr)
						}
						response := httptest.NewRecorder()
						server.ServeHTTP(response, httptest.NewRequest("GET", policy.path+shape.query, nil))
						if denied {
							status := 500
							if shape.name == "sorting" {
								status = 400
							}
							require.Equal(t, status, response.Code, response.Body.String())
						} else {
							require.Equal(t, 200, response.Code, response.Body.String())
							assertSelectorPolicyRow(t, response.Body.Bytes(), shape.id)
						}
						result, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: policy.tool, Arguments: shape.args})
						require.NoError(t, err)
						require.Equal(t, denied, result.IsError != nil && *result.IsError, "%+v", result)
						if !denied {
							payload, err := json.Marshal(result.StructuredContent)
							require.NoError(t, err)
							assertSelectorPolicyRow(t, payload, shape.id)
						}
					})
				}
			}
		})
	}
}

func assertSelectorPolicyRow(t *testing.T, payload []byte, id int) {
	t.Helper()
	var response struct {
		Data []struct {
			ID int `json:"id"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(payload, &response))
	require.Len(t, response.Data, 1, string(payload))
	require.Equal(t, id, response.Data[0].ID, string(payload))
}
