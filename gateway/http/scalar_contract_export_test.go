package http

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe"
	"github.com/viant/tagly/choice"
	"github.com/viant/xdatly/contract"
	"github.com/xuri/excelize/v2"
)

type scalarHTTPRow struct {
	ID       int     `sqlx:"id" json:"id"`
	State    string  `sqlx:"state" json:"state" csvName:"Stored state" xls:"name=Stored state" choice:"0:Pending" choice:"10:Active" enumStyle:"literal" enumOutputAlias:"0:" enumOutputAlias:"10:ACTIVE_TOKEN"`
	Kind     int     `sqlx:"kind" json:"kind" choice:"0:Image" choice:"1:Video" enumStyle:"literal"`
	Code     string  `sqlx:"code" json:"code" choice:"0:Pending" choice:"10:Active" enumStyle:"ordinal"`
	Optional *string `sqlx:"optional" json:"optional" choice:"0:Pending" choice:"10:Active" enumStyle:"literal" enumOutputAlias:"0:" enumOutputAlias:"10:ACTIVE_TOKEN"`
	Hidden   string  `sqlx:"hidden" json:"hidden" choice:"0:Pending" choice:"10:Active" enumStyle:"literal" enumOutputUnknown:"reject"`
}

type scalarHTTPCancelKey struct{}

func (*scalarHTTPRow) OnFetch(ctx context.Context) error {
	if cancel, ok := ctx.Value(scalarHTTPCancelKey{}).(context.CancelFunc); ok {
		cancel()
		return ctx.Err()
	}
	return nil
}

type scalarHTTPOutput struct {
	Rows []*scalarHTTPRow `json:"rows"`
}

// The context represents a server-selected policy, never a client query flag.
type scalarHTTPPolicyKey struct{}

func (*scalarHTTPOutput) Contract(ctx context.Context, _ bindly.Binder) (*contract.Policy, error) {
	mode, _ := ctx.Value(scalarHTTPPolicyKey{}).(string)
	return &contract.Policy{Name: "scalar-http:" + mode + ":v1", Field: func(_ string, parent reflect.Type, f *contract.Field) (*contract.Field, error) {
		if parent != reflect.TypeFor[scalarHTTPRow]() {
			return nil, nil
		}
		if f.Name == "Hidden" && mode != "visible" {
			return f.Hide(), nil
		}
		if mode == "storage" {
			return f.RemoveTag("choice").RemoveTag("enumStyle").RemoveTag("enumOutputAlias").RemoveTag("enumOutputUnknown"), nil
		}
		return nil, nil
	}}, nil
}

type scalarHTTPLogger struct{ causes []error }

func (*scalarHTTPLogger) Debug(string, ...any) {}
func (*scalarHTTPLogger) Info(string, ...any)  {}
func (*scalarHTTPLogger) Warn(string, ...any)  {}
func (l *scalarHTTPLogger) Error(_ string, args ...any) {
	for _, arg := range args {
		if err, ok := arg.(error); ok {
			l.causes = append(l.causes, err)
		}
	}
}

func scalarHTTPHandler(t *testing.T) (*Handler, *scalarHTTPLogger) {
	t.Helper()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(t.Context(),
		"CREATE TABLE scalar_exports(id INTEGER,state TEXT,kind INTEGER,code TEXT,optional TEXT,hidden TEXT,tenant INTEGER)",
		"INSERT INTO scalar_exports VALUES(1,'ACTIVE_TOKEN',1,'Active','ACTIVE_TOKEN','CUSTOMER_PRIVATE_UNKNOWN',1),(2,'',0,'Pending',NULL,'CUSTOMER_PRIVATE_UNKNOWN',1),(3,'DENIED_CUSTOMER',1,'Active',NULL,'CUSTOMER_PRIVATE_UNKNOWN',2)"))
	compiled, err := transcribe.NewCompiler().Compile(t.Context(), &transcribe.Source{Scope: "example.com/scalar-http", Name: "ScalarExport", Text: `#setting($_ = $route('/scalar-export','GET'))
#define($_ = $Fields<[]string>(query/fields).Optional().QuerySelector('p'))
#define($_ = $Rows<?>(output/view))
SELECT p.*,selector_field_selection(p,'outputOnly') FROM (SELECT id,state,kind,code,optional,hidden FROM scalar_exports WHERE tenant=1 ORDER BY id) p`})
	require.NoError(t, err)
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: compiled.Component, InputType: reflect.TypeFor[fieldModeHTTPInput](), OutputType: reflect.TypeFor[scalarHTTPOutput](), DirectViewField: "Rows"})
	require.NoError(t, err)
	execution, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
	require.NoError(t, err)
	registered, err := artifact.Registration(druntime.RegisteredComponent{Reader: execution})
	require.NoError(t, err)
	rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{registered})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	logger := &scalarHTTPLogger{}
	return NewHandler(rt, logger, "test"), logger
}

func scalarHTTPRequest(t *testing.T, h *Handler, mode, format string, fields []string) *httptest.ResponseRecorder {
	t.Helper()
	query := url.Values{"_format": {format}}
	for _, field := range fields {
		query.Add("fields", field)
	}
	request := httptest.NewRequest("GET", "/scalar-export?"+query.Encode(), nil)
	request = request.WithContext(context.WithValue(request.Context(), scalarHTTPPolicyKey{}, mode))
	response := httptest.NewRecorder()
	h.ServeHTTP(response, request)
	return response
}

func scalarHTTPTable(t *testing.T, data []byte, format string, numericColumn string) [][]string {
	t.Helper()
	if format == "csv" {
		rows, err := csv.NewReader(bytes.NewReader(data)).ReadAll()
		require.NoError(t, err)
		return rows
	}
	book, err := excelize.OpenReader(bytes.NewReader(data))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, book.Close()) })
	require.Len(t, book.GetSheetList(), 1)
	sheet := book.GetSheetList()[0]
	rows, err := book.GetRows(sheet)
	require.NoError(t, err)
	for column, header := range rows[0] {
		if header != numericColumn {
			continue
		}
		for row := 1; row < len(rows); row++ {
			cell, err := excelize.CoordinatesToCellName(column+1, row+1)
			require.NoError(t, err)
			kind, err := book.GetCellType(sheet, cell)
			require.NoError(t, err)
			require.Contains(t, []excelize.CellType{excelize.CellTypeNumber, excelize.CellTypeUnset}, kind)
		}
	}
	// Excelize omits trailing empty cells; restore the native header width.
	for row := 1; row < len(rows); row++ {
		for len(rows[row]) < len(rows[0]) {
			rows[row] = append(rows[row], "")
		}
	}
	return rows
}

func TestHTTPScalarContractExportsAndOutputOnlySelection(t *testing.T) {
	h, logger := scalarHTTPHandler(t)
	// Alternate cached policies on the same reader to catch representation bleed.
	for round, mode := range []string{"alias", "storage", "alias"} {
		for _, selected := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/%s/selected=%t", round, mode, selected), func(t *testing.T) {
				var fields []string
				if selected {
					fields = []string{"state", "kind", "code", "hidden"}
				}
				response := scalarHTTPRequest(t, h, mode, "json", fields)
				require.Equal(t, 200, response.Code, response.Body.String())
				var document struct {
					Rows []map[string]any `json:"rows"`
				}
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &document))
				require.Len(t, document.Rows, 2)
				want := []map[string]any{{"id": float64(1), "state": "Active", "kind": "Video", "code": float64(10), "optional": "Active"}, {"id": float64(2), "state": "Pending", "kind": "Image", "code": float64(0), "optional": nil}}
				if mode == "storage" {
					want[0]["state"], want[0]["kind"], want[0]["code"], want[0]["optional"] = "ACTIVE_TOKEN", float64(1), "Active", "ACTIVE_TOKEN"
					want[1]["state"], want[1]["kind"], want[1]["code"] = "", float64(0), "Pending"
				}
				if selected {
					for _, row := range want {
						delete(row, "id")
						delete(row, "optional")
					}
				}
				require.Equal(t, want, document.Rows)
				for _, format := range []string{"csv", "xls", "xlsx"} {
					t.Run(format, func(t *testing.T) {
						response := scalarHTTPRequest(t, h, mode, format, fields)
						require.Equal(t, 200, response.Code, response.Body.String())
						numeric := "Code"
						if mode == "storage" {
							numeric = "Kind"
						}
						rows := scalarHTTPTable(t, response.Body.Bytes(), format, numeric)
						columns, headers := []string{"id", "state", "kind", "code", "optional"}, []string{"ID", "Stored state", "Kind", "Code", "Optional"}
						if selected {
							columns, headers = columns[1:4], headers[1:4]
						}
						require.Len(t, rows, 3)
						require.Equal(t, headers, rows[0])
						for row, source := range document.Rows {
							var values []string
							for _, column := range columns {
								value := ""
								if source[column] != nil {
									value = fmt.Sprint(source[column])
								} else if format == "csv" {
									value = "null" // Preserve the native CSV null representation.
								}
								values = append(values, value)
							}
							require.Equal(t, values, rows[row+1], "tabular scalars must match the native JSON contract")
						}
					})
				}
			})
		}
	}
	require.Empty(t, logger.causes, "caller-hidden strict unknown values must never reach enum encoding")
}

func TestHTTPScalarContractExportErrorsRetainCauses(t *testing.T) {
	h, logger := scalarHTTPHandler(t)
	for _, format := range []string{"json", "csv", "xls", "xlsx"} {
		t.Run(format, func(t *testing.T) {
			logger.causes = nil
			response := scalarHTTPRequest(t, h, "visible", format, []string{"hidden"})
			require.Equal(t, 500, response.Code)
			require.JSONEq(t, `{"status":"error","message":"internal server error","error":"internal server error"}`, response.Body.String())
			require.NotContains(t, response.Body.String(), "CUSTOMER_PRIVATE_UNKNOWN")
			require.NotContains(t, response.Body.String(), "DENIED_CUSTOMER")
			require.ErrorIs(t, errors.Join(logger.causes...), choice.ErrUnknownOutput)
		})
	}
	logger.causes = nil
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	// Cancel inside the native row hook, after contract resolution, so this
	// exercises reader cancellation propagation through execution and HTTP.
	ctx = context.WithValue(ctx, scalarHTTPCancelKey{}, cancel)
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest("GET", "/scalar-export?_format=csv", nil).WithContext(ctx))
	require.Equal(t, 500, response.Code, response.Body.String())
	require.ErrorIs(t, errors.Join(logger.causes...), context.Canceled)
	require.NotContains(t, response.Body.String(), "CUSTOMER_PRIVATE_UNKNOWN")
	require.NotContains(t, response.Body.String(), "DENIED_CUSTOMER")
}
