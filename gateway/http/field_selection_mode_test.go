package http

import (
	"bytes"
	"context"
	"encoding/csv"
	"io"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe"
	"github.com/viant/xdatly/contract"
	"github.com/xuri/excelize/v2"
)

type fieldModeHTTPInput struct {
	Fields []string `parameter:"Fields,kind=query,in=fields"`
}
type fieldModeHTTPChild struct {
	ParentID int `sqlx:"parent_id"`
	Value    int `sqlx:"value"`
}
type fieldModeHTTPRow struct {
	ID       int                   `sqlx:"id" json:"id"`
	Name     *string               `sqlx:"name" json:"name"`
	Kind     string                `sqlx:"kind" json:"-" internal:"true"`
	Secret   string                `sqlx:"secret" json:"secret"`
	Class    string                `sqlx:"-" json:"class"`
	Assigned bool                  `sqlx:"-" json:"assigned"`
	Children []*fieldModeHTTPChild `view:"children,table=children" on:"ID:id=ParentID:parent_id" sql:"SELECT parent_id,value FROM children" json:"-" internal:"true"`
}

func (r *fieldModeHTTPRow) OnFetch(context.Context) error {
	r.Class = "unsupported"
	if r.Kind == "banner" && r.Secret == "PRIVATE" {
		r.Class = "banner"
	}
	return nil
}
func (r *fieldModeHTTPRow) OnRelation(context.Context) { r.Assigned = len(r.Children) > 0 }

type fieldModeHTTPOutput struct {
	Rows []*fieldModeHTTPRow `json:"rows"`
}

func (*fieldModeHTTPOutput) Contract(context.Context, bindly.Binder) (*contract.Policy, error) {
	return &contract.Policy{Name: "field-mode:hide:r1", Field: func(_ string, _ reflect.Type, f *contract.Field) (*contract.Field, error) {
		if f.Name == "Secret" {
			return f.Hide(), nil
		}
		return nil, nil
	}}, nil
}
func fieldModeHTTPHandler(t *testing.T) *Handler {
	t.Helper()
	ctx := t.Context()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER,name TEXT,kind TEXT,secret TEXT,tenant INTEGER)", "CREATE TABLE children(parent_id INTEGER,value INTEGER)", "INSERT INTO parents VALUES(1,'one','banner','PRIVATE',1),(2,NULL,'banner','PRIVATE',1),(3,'denied','banner','PRIVATE',2)", "INSERT INTO children VALUES(1,10),(3,30)"))
	compiled, err := transcribe.NewCompiler().Compile(ctx, &transcribe.Source{Scope: "example.com/fieldmode", Name: "FieldMode", Text: `#setting($_ = $route('/field-mode','GET'))
#define($_ = $Fields<[]string>(query/fields).Optional().QuerySelector('p'))
#define($_ = $Rows<?>(output/view))
SELECT p.id,p.name,p.kind,p.secret,selector_field_selection(p,'outputOnly') FROM (SELECT id,name,kind,secret FROM parents WHERE tenant=1) p`})
	require.NoError(t, err)
	output := reflect.TypeFor[fieldModeHTTPOutput]()
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: compiled.Component, InputType: reflect.TypeFor[fieldModeHTTPInput](), OutputType: output, DirectViewField: "Rows"})
	require.NoError(t, err)
	execution, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
	require.NoError(t, err)
	registered, err := artifact.Registration(druntime.RegisteredComponent{Reader: execution})
	require.NoError(t, err)
	runtime, err := druntime.NewRuntime([]*druntime.RegisteredComponent{registered})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Shutdown(context.Background())) })
	_, smokeErr := runtime.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: artifact.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/field-mode"}}, Input: &fieldModeHTTPInput{Fields: []string{"class"}}})
	require.NoError(t, smokeErr)
	return NewHandler(runtime, nil, "test")
}

func TestHTTPOutputOnlyFieldSelectionContractAndFormats(t *testing.T) {
	h := fieldModeHTTPHandler(t)
	query := url.Values{"fields": {"id", "class", "assigned", "secret"}}
	for _, format := range []string{"json", "csv", "xls", "xlsx"} {
		t.Run(format, func(t *testing.T) {
			query.Set("_format", format)
			response := httptest.NewRecorder()
			h.ServeHTTP(response, httptest.NewRequest("GET", "/field-mode?"+query.Encode(), nil))
			require.Equal(t, 200, response.Code, response.Body.String())
			if format == "json" {
				require.JSONEq(t, `{"rows":[{"id":1,"class":"banner","assigned":true},{"id":2,"class":"banner","assigned":false}]}`, response.Body.String())
				return
			}
			var rows [][]string
			if format == "csv" {
				rowsErr := error(nil)
				rows, rowsErr = csv.NewReader(strings.NewReader(response.Body.String())).ReadAll()
				require.NoError(t, rowsErr)
			} else {
				workbook, err := excelize.OpenReader(bytes.NewReader(response.Body.Bytes()))
				require.NoError(t, err)
				defer workbook.Close()
				for _, sheet := range workbook.GetSheetList() {
					content, err := workbook.GetRows(sheet)
					require.NoError(t, err)
					rows = append(rows, content...)
				}
			}
			require.Len(t, rows, 3)
			require.Len(t, rows[0], 3, "selection and Dynamic Contract compose before tabular encoding")
			text := ""
			for _, row := range rows {
				text += strings.Join(row, "|") + "\n"
			}
			require.Contains(t, text, "banner")
			require.Contains(t, strings.ToLower(text), "true")
			require.Contains(t, strings.ToLower(text), "false")
			require.NotContains(t, text, "unsupported")
			require.NotContains(t, text, "PRIVATE")
			require.NotContains(t, strings.ToLower(text), "secret")
			require.NotContains(t, strings.ToLower(text), "name")
		})
	}
	unknown := httptest.NewRecorder()
	h.ServeHTTP(unknown, httptest.NewRequest("GET", "/field-mode?fields=missing", nil))
	require.Equal(t, 400, unknown.Code, unknown.Body.String())
	require.NotContains(t, unknown.Body.String(), "PRIVATE")
}

func TestHTTPOutputOnlyFieldSelectionConcurrentIsolation(t *testing.T) {
	h := fieldModeHTTPHandler(t)
	var wg sync.WaitGroup
	type outcome struct {
		body   string
		status int
		field  string
	}
	results := make(chan outcome, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			field := []string{"class", "name"}[i%2]
			response := httptest.NewRecorder()
			h.ServeHTTP(response, httptest.NewRequest("GET", "/field-mode?fields="+field, nil))
			body, _ := io.ReadAll(response.Result().Body)
			results <- outcome{string(body), response.Code, field}
		}(i)
	}
	wg.Wait()
	close(results)
	for result := range results {
		require.Equal(t, 200, result.status, result.body)
		if result.field == "class" {
			require.JSONEq(t, `{"rows":[{"class":"banner"},{"class":"banner"}]}`, result.body)
		} else {
			require.JSONEq(t, `{"rows":[{"name":"one"},{"name":null}]}`, result.body)
		}
	}
}
