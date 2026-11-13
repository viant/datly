package http

import (
	"bytes"
	"context"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/datly/bootstrap"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/xdatly/contract"
	"github.com/xuri/excelize/v2"
)

type exportContractInput struct{}
type exportContractRow struct {
	Public string
	Secret string
	Child  *exportContractChild
}
type exportContractChild struct {
	Public string
	Secret string
}
type exportContractOutput struct{ Data []*exportContractRow }

func (*exportContractOutput) Contract(context.Context, bindly.Binder) (*contract.Policy, error) {
	return &contract.Policy{Name: "export:hide:r1", Field: func(_ string, _ reflect.Type, f *contract.Field) (*contract.Field, error) {
		if f.Name == "Secret" {
			return f.Hide(), nil
		}
		return nil, nil
	}}, nil
}
func TestHTTPContractCSVAndSpreadsheetExports(t *testing.T) {
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "ContractExport"}, Routes: []*spec.Route{{Method: "GET", Path: "/contract-export"}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[exportContractInput](), OutputType: reflect.TypeFor[exportContractOutput]()})
	require.NoError(t, err)
	value := &exportContractOutput{Data: []*exportContractRow{{Public: "public", Secret: "PRIVATE"}}}
	registered, err := artifact.Registration(registry.RegisteredComponent{Handler: custom.NewFunc(func(context.Context, *exportContractInput) (*exportContractOutput, error) { return value, nil })})
	require.NoError(t, err)
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{registered})
	require.NoError(t, err)
	defer runtime.Shutdown(context.Background())
	handler := NewHandler(runtime, nil, "test")
	for _, format := range []string{"csv", "xls", "xlsx"} {
		t.Run(format, func(t *testing.T) {
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest("GET", "/contract-export?_format="+format, nil))
			require.Equal(t, 200, rec.Code, rec.Body.String())
			text := rec.Body.String()
			if format != "csv" {
				workbook, err := excelize.OpenReader(bytes.NewReader(rec.Body.Bytes()))
				require.NoError(t, err)
				defer workbook.Close()
				text = ""
				for _, sheet := range workbook.GetSheetList() {
					rows, err := workbook.GetRows(sheet)
					require.NoError(t, err)
					for _, row := range rows {
						text += strings.Join(row, "|")
					}
				}
			}
			require.Contains(t, text, "public")
			require.NotContains(t, text, "PRIVATE")
			require.NotContains(t, strings.ToLower(text), "secret")
		})
	}
	require.Equal(t, "PRIVATE", value.Data[0].Secret)
	for _, format := range []string{"xml", "tabular"} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest("GET", "/contract-export?_format="+format, nil))
		require.Equal(t, 406, rec.Code)
	}
}
