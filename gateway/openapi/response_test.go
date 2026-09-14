package openapi_test

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	gatewayhttp "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/gateway/openapi"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
	"github.com/xuri/excelize/v2"
)

type readRow struct {
	ID   int    `sqlx:"id" json:"id" csvName:"id"`
	Name string `sqlx:"name" json:"name" csvName:"name"`
}
type readOutput struct {
	Rows []readRow `json:"data"`
}

func TestBootstrapReaderDocumentMediaAndAPIKeySQLite(t *testing.T) {
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER, name TEXT)", "INSERT INTO records VALUES(7, 'Example')"))
	for _, tc := range []struct {
		name, format, routeFormat, media, schemaType, schemaFormat string
		secure                                                     bool
	}{
		{name: "JSON", format: "json", media: "application/json", schemaType: "object"},
		{name: "API-key JSON", format: "json", media: "application/json", schemaType: "object", secure: true},
		{name: "CSV", format: "csv", media: "text/csv", schemaType: "string"},
		{name: "route XLSX overrides JSON", format: "json", routeFormat: "xlsx", media: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", schemaType: "string", schemaFormat: "binary"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Records"},
				Settings: &spec.Settings{Format: tc.format}, Routes: []*spec.Route{{Method: "GET", Path: "/records", Marshaller: tc.routeFormat}},
				RootView:   &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: "SELECT id, name FROM records"}},
				Parameters: []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}},
			}
			if tc.secure {
				component.Routes[0].APIKeyHeader = "X-API-Key"
				component.Routes[0].APIKeyValue = "private-secret"
			}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[readOutput]()})
			require.NoError(t, err)
			reader, err := sqlreader.NewExecution(sqlreader.Config{Component: artifact.Component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[readOutput](), Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: h.DB}})
			require.NoError(t, err)
			entry, err := artifact.Registration(registry.RegisteredComponent{Reader: reader})
			require.NoError(t, err)
			doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
			require.NoError(t, err)
			operation := doc.Paths["/records"].Get
			require.Len(t, operation.Responses, 1, "no arbitrary default errors")
			response := operation.Responses["200"].Content[tc.media]
			require.NotNil(t, response)
			a := documentAssertion{t, doc}
			schema := a.deref(response.Schema)
			require.Equal(t, tc.schemaType, schema.Type)
			require.Equal(t, tc.schemaFormat, schema.Format)
			if tc.schemaType == "object" {
				row := a.deref(schema.Properties["data"].Items)
				require.Equal(t, "integer", row.Properties["id"].Type)
				require.Equal(t, "string", row.Properties["name"].Type)
			}
			encoded, err := json.Marshal(doc)
			require.NoError(t, err)
			require.NotContains(t, string(encoded), "private-secret")
			require.NotContains(t, string(encoded), "bearer")
			if tc.secure {
				require.Len(t, doc.Components.SecuritySchemes, 1)
				require.Len(t, *operation.Security, 1)
				for _, scheme := range doc.Components.SecuritySchemes {
					require.Equal(t, "apiKey", scheme.Type)
					require.Equal(t, "X-API-Key", scheme.Name)
					require.Equal(t, "header", scheme.In)
				}
			} else {
				require.Empty(t, doc.Components.SecuritySchemes)
				require.Nil(t, operation.Security)
			}
			rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
			require.NoError(t, err)
			if tc.secure {
				recorder := httptest.NewRecorder()
				gatewayhttp.NewHandler(rt, nil, "test").ServeHTTP(recorder, httptest.NewRequest("GET", "/records", nil))
				require.Equal(t, 403, recorder.Code)
			}
			req := httptest.NewRequest("GET", "/records", nil)
			if tc.secure {
				req.Header.Set("X-API-Key", "private-secret")
			}
			recorder := httptest.NewRecorder()
			gatewayhttp.NewHandler(rt, nil, "test").ServeHTTP(recorder, req)
			require.Equal(t, 200, recorder.Code, recorder.Body.String())
			require.Equal(t, tc.media, recorder.Header().Get("Content-Type"))
			switch tc.media {
			case "application/json":
				var result readOutput
				require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &result))
				require.Equal(t, []readRow{{7, "Example"}}, result.Rows)
			case "text/csv":
				rows, err := csv.NewReader(strings.NewReader(recorder.Body.String())).ReadAll()
				require.NoError(t, err)
				require.Equal(t, []string{"7", "Example"}, rows[1])
			default:
				book, err := excelize.OpenReader(strings.NewReader(recorder.Body.String()))
				require.NoError(t, err)
				defer book.Close()
				rows, err := book.GetRows(book.GetSheetName(0))
				require.NoError(t, err)
				require.Equal(t, []string{"7", "Example"}, rows[1])
			}
		})
	}
}
