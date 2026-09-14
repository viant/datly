package http

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe"
)

type sourceGuardHTTPInput struct {
	Fields       []string `parameter:"Fields,kind=query,in=fields"`
	OrderBy      string   `parameter:"OrderBy,kind=query,in=orderby"`
	ChildFields  []string `parameter:"ChildFields,kind=query,in=child_fields"`
	ChildOrderBy string   `parameter:"ChildOrderBy,kind=query,in=child_orderby"`
}
type sourceGuardHTTPChild struct {
	B_ID     int    `sqlx:"b_id" json:"b_id,omitempty"`
	ID       int    `sqlx:"id" json:"id"`
	ParentID int    `sqlx:"parent_id" json:"parentId"`
	Name     string `sqlx:"name" json:"name"`
	Secret   string `sqlx:"secret" json:"secret,omitempty"`
}
type sourceGuardHTTPOutput struct {
	Rows []*sourceGuardHTTPRow `json:"rows"`
}

type sourceGuardHTTPRow struct {
	BID      int                     `sqlx:"bid" json:"bid,omitempty"`
	ID       int                     `sqlx:"id" json:"id"`
	Name     string                  `sqlx:"name" json:"name"`
	Total    int                     `sqlx:"total" json:"total"`
	Secret   string                  `sqlx:"secret" json:"secret,omitempty"`
	Children []*sourceGuardHTTPChild `view:"children,table=children,selectorProjection=true" on:"ID:id=ParentID:parent_id" json:"children,omitempty"`
}

func sourceGuardHTTPHandler(t *testing.T, cube bool, authoredAliases ...bool) *Handler {
	t.Helper()
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER,name TEXT,price INTEGER,secret TEXT)", "CREATE TABLE children(id INTEGER,parent_id INTEGER,name TEXT,secret TEXT)", "INSERT INTO parents VALUES(1,'first',2,'hidden'),(2,'second',3,'hidden')", "INSERT INTO children VALUES(10,1,'child','hidden'),(20,2,'other','hidden')"))
	source := `#setting($_ = $route('/guard','GET'))
#define($_ = $Fields<[]string>(query/fields).Optional().QuerySelector('parents'))
#define($_ = $OrderBy<string>(query/orderby).Optional().QuerySelector('parents'))
#define($_ = $Fields<[]string>(query/child_fields).Optional().QuerySelector('children'))
#define($_ = $OrderBy<string>(query/child_orderby).Optional().QuerySelector('children'))
#define($_ = $Rows<?>(output/view))
SELECT id,name FROM parents`
	if cube {
		source = source[:len(source)-len("SELECT id,name FROM parents")] + "SELECT name,SUM(price) AS total FROM parents GROUP BY name"
	}
	if len(authoredAliases) > 0 && authoredAliases[0] {
		source = strings.Replace(source, "SELECT id,name FROM parents", "SELECT id,id AS bid,name FROM parents", 1)
	}
	compiled, err := transcribe.NewCompiler().Compile(ctx, &transcribe.Source{Scope: "example.com/sourceguard", Name: "Guard", Text: source})
	require.NoError(t, err)
	policy := &spec.Selector{AllowFields: true, AllowOrderBy: true, Orderable: []spec.FieldPath{"id", "name", "total", "secret"}, OrderAliases: map[string]spec.FieldPath{"private": "secret", "display": "name"}}
	if len(authoredAliases) > 0 && authoredAliases[0] {
		policy.Orderable = append(policy.Orderable, "bid", "b_id")
	}
	compiled.Component.RootView.Name = "parents"
	compiled.Component.RootView.Selector = policy
	if cube {
		groupable := true
		compiled.Component.RootView.Groupable = &groupable
	}
	output := reflect.TypeFor[sourceGuardHTTPOutput]()
	// Current main requires unique canonical parameter names. The separate
	// multiview patch owns repeated authored selector-name lowering.
	for _, param := range spec.EffectiveParameters(compiled.Component.Parameters) {
		switch param.Source.Name {
		case "child_fields":
			param.Name = "ChildFields"
		case "child_orderby":
			param.Name = "ChildOrderBy"
		}
	}
	inputType := reflect.TypeFor[sourceGuardHTTPInput]()
	require.NoError(t, err)
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: compiled.Component, InputType: inputType, OutputType: output, DirectViewField: "Rows"})
	require.NoError(t, err)
	child := artifact.Reader.Root.View.Relations[0].Of.View
	child.Spec.Source = &spec.ViewSource{SQL: "SELECT id,parent_id,name FROM children"}
	child.Spec.Selector = policy
	if len(authoredAliases) > 0 && authoredAliases[0] {
		child.Spec.Source.SQL = "SELECT id,id AS b_id,parent_id,name FROM children"
		policy.Orderable = append(policy.Orderable, "bid", "b_id")
	}
	execution, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
	require.NoError(t, err)
	runtime, err := druntime.NewRuntime([]*druntime.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: output, Reader: execution}})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Shutdown(ctx)) })
	return NewHandler(runtime, nil, "test")
}

func TestHTTPSelectorSourceGuardSQLite(t *testing.T) {
	for _, cube := range []bool{false, true} {
		h := sourceGuardHTTPHandler(t, cube)
		for _, tc := range []struct{ name, key, value string }{
			{"root unknown field", "fields", "missing"}, {"root stale typed field", "fields", "secret"}, {"root allowed unknown order", "orderby", "secret"}, {"root alias unknown order", "orderby", "private"}, {"root invalid ordinal", "orderby", "99"},
			{"child unknown field", "child_fields", "missing"}, {"child stale typed field", "child_fields", "secret"}, {"child allowed unknown order", "child_orderby", "secret"}, {"child alias unknown order", "child_orderby", "private"}, {"child invalid ordinal", "child_orderby", "99"},
		} {
			if cube && len(tc.key) > 5 && tc.key[:6] == "child_" {
				continue
			}
			t.Run(tc.name+map[bool]string{true: " cube", false: ""}[cube], func(t *testing.T) {
				response := httptest.NewRecorder()
				h.ServeHTTP(response, httptest.NewRequest("GET", "/guard?"+url.Values{tc.key: {tc.value}}.Encode(), nil))
				require.GreaterOrEqual(t, response.Code, 400, response.Body.String())
				require.Less(t, response.Code, 600)
				require.NotContains(t, response.Body.String(), "hidden")
			})
		}
		fields, order := "name", "display DESC"
		if cube {
			fields = "total"
			order = "1 DESC"
		}
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest("GET", "/guard?"+url.Values{"fields": {fields}, "orderby": {order}}.Encode(), nil))
		require.Equal(t, 200, response.Code, response.Body.String())
		if cube {
			require.Contains(t, response.Body.String(), `"total":5`)
		} else {
			require.Contains(t, response.Body.String(), `"name":"second"`)
		}
		response = httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest("GET", "/guard?"+url.Values{"fields": {fields}, "orderby": {"2"}}.Encode(), nil))
		require.GreaterOrEqual(t, response.Code, 400, response.Body.String())
	}
}

func TestHTTPDeclaredOutputNames(t *testing.T) {
	h := sourceGuardHTTPHandler(t, false)
	for _, prefix := range []string{"", "child_"} {
		for _, property := range []string{"fields", "orderby"} {
			for _, name := range []string{"b.id", "b_id", "bid"} {
				response := httptest.NewRecorder()
				h.ServeHTTP(response, httptest.NewRequest("GET", "/guard?"+url.Values{prefix + property: {name}}.Encode(), nil))
				require.GreaterOrEqual(t, response.Code, 400, prefix+property+"="+name+" "+response.Body.String())
			}
		}
	}
	// Root and subview can independently expose id.
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest("GET", "/guard", nil))
	require.Equal(t, 200, response.Code, response.Body.String())
	var payload sourceGuardHTTPOutput
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &payload))
	require.NotEmpty(t, payload.Rows)
	require.Equal(t, 1, payload.Rows[0].ID)
	require.NotEmpty(t, payload.Rows[0].Children)
	require.Equal(t, 10, payload.Rows[0].Children[0].ID)
	authored := sourceGuardHTTPHandler(t, false, true)
	for _, tc := range []struct{ query, want string }{
		{"fields=bid&orderby=bid", `"bid":1`},
		{"fields=id&fields=Children&child_fields=b_id&child_orderby=b_id", `"b_id":10`},
	} {
		response = httptest.NewRecorder()
		authored.ServeHTTP(response, httptest.NewRequest("GET", "/guard?"+tc.query, nil))
		require.Equal(t, 200, response.Code, response.Body.String())
		require.Contains(t, response.Body.String(), tc.want)
	}
}
