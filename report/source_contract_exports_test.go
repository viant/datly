package report

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/mcp/invocation"
	"github.com/viant/datly/output"
	"github.com/viant/datly/spec"
	"github.com/xuri/excelize/v2"
)

func TestSourceContractJSONCSVXLSXPreservesRESTAndSourceValues(t *testing.T) {
	h := (groupedReportHarnessConfig{output: reflect.TypeFor[sourcePolicyOutput](), report: []func(*spec.ReportSettings){func(s *spec.ReportSettings) { s.Compose = &spec.CubeComposeSettings{Enabled: true} }}}).build(t)
	ctx := invocation.WithMCPContext(t.Context(), nil)
	plan, ok := h.service.Catalog().Tool("SpendCubeCompose")
	require.True(t, ok)
	effective, err := h.runtime.ResolveContract(ctx, plan.Target(), requestprovider.NewValues())
	source := effective.SourceContract
	require.NoError(t, err)
	base, err := (output.Compiler{}).Compile(output.CompileInput{Type: source.Output, DataField: "Rows"})
	require.NoError(t, err)
	native, err := base.WithContract(source.Output, source.OutputRepresentation)
	require.NoError(t, err)
	public, err := native.WithMCPVisibility()
	require.NoError(t, err)
	value := &sourcePolicyOutput{Rows: []*sourcePolicyRow{{AccountID: 1, Region: "PRIVATE_REGION", TotalSpend: 12345.67, OrderCount: 2}}}
	for _, format := range []string{"json", "csv", "xlsx"} {
		t.Run(format, func(t *testing.T) {
			encoded, err := public.Encode(ctx, format, value)
			require.NoError(t, err)
			text := string(encoded.Data)
			if format == "xlsx" {
				book, err := excelize.OpenReader(bytes.NewReader(encoded.Data))
				require.NoError(t, err)
				defer book.Close()
				text = ""
				for _, sheet := range book.GetSheetList() {
					rows, err := book.GetRows(sheet)
					require.NoError(t, err)
					for _, row := range rows {
						text += strings.Join(row, "|") + "\n"
					}
				}
			}
			require.NotContains(t, text, "PRIVATE_REGION")
			require.NotContains(t, text, "12345")
			require.NotContains(t, strings.ToLower(text), "totalspend")
			rest, err := native.Encode(context.Background(), format, value)
			require.NoError(t, err)
			restText := string(rest.Data)
			if format == "xlsx" {
				book, err := excelize.OpenReader(bytes.NewReader(rest.Data))
				require.NoError(t, err)
				defer book.Close()
				restText = ""
				for _, sheet := range book.GetSheetList() {
					rows, err := book.GetRows(sheet)
					require.NoError(t, err)
					for _, row := range rows {
						restText += strings.Join(row, "|") + "\n"
					}
				}
			}
			require.Contains(t, restText, "PRIVATE_REGION")
			require.Contains(t, restText, "12345")
		})
	}
	require.Equal(t, "PRIVATE_REGION", value.Rows[0].Region)
	require.Equal(t, 12345.67, value.Rows[0].TotalSpend)
}
