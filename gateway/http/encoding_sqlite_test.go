package http

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	stdhttp "net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
	"github.com/xuri/excelize/v2"
)

type encodingRow struct {
	ID     int    `sqlx:"id" json:"id" csvName:"id"`
	Name   string `sqlx:"name" json:"name" csvName:"name"`
	Secret string `sqlx:"secret" json:"secret,omitempty" csvName:"secret"`
}
type encodingEnvelope struct {
	Rows []encodingRow `json:"data"`
}

type httpOutputMarshaller struct{}

func (*httpOutputMarshaller) Marshal(value any) ([]byte, error) {
	output := value.(*encodingEnvelope)
	return json.Marshal(struct {
		Count int    `json:"count"`
		First string `json:"first"`
	}{len(output.Rows), output.Rows[0].Name})
}

type failingOutputMarshaller struct{}

func (*failingOutputMarshaller) Marshal(any) ([]byte, error) {
	return nil, errors.New("private codec diagnostic")
}

func TestHTTPCustomOutputMarshallerSQLite(t *testing.T) {
	h := sqlite.New(t)
	for _, tt := range []struct {
		name   string
		typeOf reflect.Type
		status int
	}{
		{"custom", reflect.TypeOf(httpOutputMarshaller{}), 200},
		{"error", reflect.TypeOf(failingOutputMarshaller{}), 500},
	} {
		t.Run(tt.name, func(t *testing.T) {
			types := typecatalog.NewCatalog()
			descriptor := xshape.Linked(tt.typeOf).Descriptor()
			if err := types.Register(typecatalog.TypeOriginPackage, descriptor); err != nil {
				t.Fatal(err)
			}
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Custom"}, Settings: &spec.Settings{JSONMarshalType: descriptor.Key()}, Routes: []*spec.Route{{Method: "GET", Path: "/custom"}}}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeOf(encodingEnvelope{}), Types: types})
			if err != nil {
				t.Fatal(err)
			}
			runtime, err := druntime.NewRuntime([]*druntime.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeOf(encodingEnvelope{}), Handler: customhandler.NewFunc[struct{}, encodingEnvelope](func(ctx context.Context, _ *struct{}) (*encodingEnvelope, error) {
				rows, err := h.ReadQuery(ctx, sqlite.Query{SQL: "SELECT 7 AS id, 'Alpha' AS name, '' AS secret"}, reflect.TypeOf([]encodingRow{}))
				if err != nil {
					return nil, err
				}
				return &encodingEnvelope{Rows: rows.([]encodingRow)}, nil
			})}})
			if err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			NewHandler(runtime, nil, "test").ServeHTTP(response, httptest.NewRequest("GET", "/custom", nil))
			if response.Code != tt.status {
				t.Fatalf("status %d body %s", response.Code, response.Body.String())
			}
			if tt.status == 200 && response.Body.String() != `{"count":1,"first":"Alpha"}` {
				t.Fatalf("custom result %s", response.Body.String())
			}
			if tt.status == 500 && (!strings.Contains(response.Body.String(), "internal server error") || strings.Contains(response.Body.String(), "private")) {
				t.Fatalf("unsafe error %s", response.Body.String())
			}
		})
	}
}

func TestHTTPAuthoredOutputFormatsSQLite(t *testing.T) {
	h := sqlite.New(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,name TEXT,secret TEXT)", "INSERT INTO records VALUES(1,'Alpha','private-1'),(2,'Beta','private-2')"); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name, format, route, query, content string
		status                              int
		exclude                             bool
	}{
		{"default JSON", "", "", "", "application/json", 200, false},
		{"authored CSV", "csv", "", "", "text/csv", 200, false},
		{"query XML", "json", "", "xml", "application/xml", 200, false},
		{"authored XLS", "xls", "", "", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 200, false},
		{"route tabular", "json", "tabular", "", "application/json", 200, false},
		{"query precedence", "csv", "xml", "json", "application/json", 200, false},
		{"unknown format", "json", "", "invalid", "application/json", 400, false},
		{"JSON exclusion", "json", "", "", "application/json", 200, true},
		{"CSV exclusion", "csv", "", "", "text/csv", 200, true},
		{"XML exclusion", "xml", "", "", "application/xml", 200, true},
		{"tabular exclusion", "tabular", "", "", "application/json", 200, true},
		{"XLS exclusion", "xls", "", "", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 200, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Records"}, RootView: &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: "SELECT id,name,secret FROM records ORDER BY id"}}, Settings: &spec.Settings{Format: tt.format}, Routes: []*spec.Route{{Method: "GET", Path: "/records", Marshaller: tt.route}}, Parameters: []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}}}
			if tt.exclude {
				component.Settings.Output = &spec.OutputSettings{Exclude: []string{"Secret"}, Title: "Records"}
			}

			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeOf(encodingEnvelope{})})
			if err != nil {
				t.Fatal(err)
			}
			reader, err := sqlreader.NewExecution(sqlreader.Config{Component: artifact.Component, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeOf(encodingEnvelope{}), Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: h.DB}})
			if err != nil {
				t.Fatal(err)
			}
			runtime, err := druntime.NewRuntime([]*druntime.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeOf(encodingEnvelope{}), Reader: reader}})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(stdhttp.MethodGet, "/records", nil)
			if tt.query != "" {
				query := request.URL.Query()
				query.Set("_format", tt.query)
				request.URL.RawQuery = query.Encode()
			}
			response := httptest.NewRecorder()
			NewHandler(runtime, nil, "test").ServeHTTP(response, request)
			if response.Code != tt.status || response.Header().Get("Content-Type") != tt.content {
				t.Fatalf("HTTP %d %s: %s", response.Code, response.Header().Get("Content-Type"), response.Body.String())
			}
			if tt.status != 200 {
				return
			}
			format := tt.format
			if format == "" {
				format = "json"
			}
			if tt.route != "" {
				format = tt.route
			}
			if tt.query != "" {
				format = tt.query
			}
			if tt.exclude && format != "xls" && bytes.Contains(response.Body.Bytes(), []byte("private-")) {
				t.Fatalf("excluded value leaked: %s", response.Body.String())
			}
			switch format {
			case "json":
				var actual encodingEnvelope
				if err = json.Unmarshal(response.Body.Bytes(), &actual); err != nil {
					t.Fatal(err)
				}
				expected := []encodingRow{{1, "Alpha", "private-1"}, {2, "Beta", "private-2"}}
				if tt.exclude {
					expected[0].Secret = ""
					expected[1].Secret = ""
				}
				if !reflect.DeepEqual(actual.Rows, expected) {
					t.Fatalf("JSON rows %+v", actual)
				}
			case "csv":
				records, err := csv.NewReader(bytes.NewReader(response.Body.Bytes())).ReadAll()
				if err != nil {
					t.Fatal(err)
				}
				if len(records) != 3 || records[1][0] != "1" || records[1][1] != "Alpha" || records[2][1] != "Beta" {
					t.Fatalf("CSV rows %#v", records)
				}
			case "tabular":
				var actual struct {
					Data [][]any `json:"data"`
				}
				if err = json.Unmarshal(response.Body.Bytes(), &actual); err != nil {
					t.Fatal(err)
				}
				if len(actual.Data) != 3 || actual.Data[1][1] != "Alpha" || actual.Data[2][1] != "Beta" {
					t.Fatalf("tabular rows %#v: %s", actual, response.Body.String())
				}
			case "xml":
				decoder := xml.NewDecoder(bytes.NewReader(response.Body.Bytes()))
				var text strings.Builder
				for {
					token, err := decoder.Token()
					if err == io.EOF {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					if value, ok := token.(xml.CharData); ok {
						text.Write(value)
					}
				}
				if !strings.Contains(text.String(), "Alpha") || !strings.Contains(text.String(), "Beta") {
					t.Fatalf("XML lost data: %s", response.Body.String())
				}
			case "xls":
				workbook, err := excelize.OpenReader(bytes.NewReader(response.Body.Bytes()))
				if err != nil {
					t.Fatal(err)
				}
				defer workbook.Close()
				var cells []string
				for _, sheet := range workbook.GetSheetList() {
					rows, err := workbook.GetRows(sheet)
					if err != nil {
						t.Fatal(err)
					}
					for _, row := range rows {
						cells = append(cells, row...)
					}
				}
				if tt.exclude && strings.Contains(strings.Join(cells, "|"), "private-") {
					t.Fatalf("excluded XLS value leaked: %v", cells)
				}
				if !strings.Contains(strings.Join(cells, "|"), "Alpha") || !strings.Contains(strings.Join(cells, "|"), "Beta") {
					t.Fatalf("XLS lost cells: %v", cells)
				}
			}
		})
	}
}
