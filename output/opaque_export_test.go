package output

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	structjson "github.com/viant/structology/encoding/json"
	"reflect"
	"strings"
	"testing"
)

type opaqueExportRow struct {
	ID     int            `json:"id"`
	Errors []any          `json:"errors"`
	Map    map[string]any `json:"map"`
}
type opaqueExportEnvelope struct {
	Data   []opaqueExportRow `json:"data"`
	Errors []any             `json:"errors"`
}

func TestOpaqueExportNativeCells(t *testing.T) {
	for _, transformed := range []bool{false, true} {
		t.Run(fmt.Sprint(transformed), func(t *testing.T) {
			value := opaqueExportEnvelope{Data: []opaqueExportRow{{ID: 1, Errors: nil}, {ID: 2, Errors: []any{}, Map: map[string]any{}}, {ID: 3, Errors: []any{nil, "雪", 3, map[string]any{"child": []any{true, "x"}}}, Map: map[string]any{"typed": &struct {
				Name string `json:"name"`
			}{"leaf"}}}}, Errors: []any{"ENVELOPE"}}
			input := CompileInput{Type: reflect.TypeOf(value), DataField: "Data"}
			if transformed {
				input.Component = &spec.Component{Settings: &spec.Settings{CaseFormat: "lc", Output: &spec.OutputSettings{NilSlicePolicy: "empty_array"}}}
			}
			plan, err := (Compiler{}).Compile(input)
			require.NoError(t, err)
			require.NoError(t, plan.formatError)
			body, err := plan.Encode(t.Context(), "json", value)
			require.NoError(t, err)
			var native struct {
				Data []struct {
					Errors json.RawMessage
					Map    json.RawMessage
				}
			}
			require.NoError(t, json.Unmarshal(body.Data, &native))
			for _, format := range []string{"csv", "xls", "xlsx"} {
				result, err := plan.Encode(t.Context(), format, value)
				require.NoError(t, err)
				rows := scalarExportRows(t, result, format)
				var cells []string
				for _, row := range rows {
					cells = append(cells, row...)
				}
				for _, row := range native.Data {
					require.Contains(t, cells, string(row.Errors))
					require.Contains(t, cells, string(row.Map))
				}
				if format != "csv" {
					require.Contains(t, cells, `["ENVELOPE"]`)
				} else {
					require.NotContains(t, cells, `["ENVELOPE"]`)
				}
			}
			cycle := map[string]any{}
			cycle["cycle"] = cycle
			value.Errors = []any{cycle}
			for _, ctx := range []context.Context{t.Context(), recursiveSelection(t, value, structjson.FieldSelection{Path: []string{"Data"}, Fields: []string{"ID", "Errors"}})} {
				result, err := plan.Encode(ctx, "csv", value)
				require.NoError(t, err)
				require.NotEmpty(t, result.Data)
			}
			for _, format := range []string{"xls", "xlsx"} {
				result, err := plan.Encode(t.Context(), format, value)
				var unsupported *json.UnsupportedValueError
				require.ErrorAs(t, err, &unsupported)
				require.Empty(t, result.Data)
			}
		})
	}
}
func TestOpaqueExportErrorsAndHiding(t *testing.T) {
	value := []opaqueExportRow{{Errors: []any{recursiveFailScalar("x")}}}
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeOf(value)})
	require.NoError(t, err)
	for _, format := range []string{"csv", "xls", "xlsx"} {
		result, err := plan.Encode(t.Context(), format, value)
		require.ErrorIs(t, err, recursiveCodecFailure)
		require.Empty(t, result.Data)
	}
	hidden, err := plan.WithRepresentation(recursiveExportMetadata(t, plan.typeOf, "Errors"))
	require.NoError(t, err)
	for _, format := range []string{"csv", "xls", "xlsx"} {
		result, err := hidden.Encode(t.Context(), format, value)
		require.NoError(t, err)
		require.NotEmpty(t, result.Data)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		result, err = plan.Encode(ctx, format, value)
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, result.Data)
	}
}

type opaqueExportCustom []any

func (opaqueExportCustom) MarshalJSON() ([]byte, error) { return []byte(`{"custom":"kept"}`), nil }

type opaqueExportEnum struct {
	State string `json:"state" choice:"1:On" enumStyle:"literal" enumOutputUnknown:"reject"`
}

func TestOpaqueExportCustomAndEnumOwners(t *testing.T) {
	type row struct {
		Custom  opaqueExportCustom
		Payload any
		Map     map[string]opaqueExportEnum
	}
	value := []row{{Custom: opaqueExportCustom{recursiveFailScalar("ignored")}, Payload: opaqueExportEnum{State: "On"}, Map: map[string]opaqueExportEnum{"a": {State: "On"}}}}
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeOf(value), Component: &spec.Component{Settings: &spec.Settings{CaseFormat: "lc"}}})
	require.NoError(t, err)
	native, err := plan.Encode(t.Context(), "json", value)
	require.NoError(t, err)
	var expected []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(native.Data, &expected))
	for _, format := range []string{"csv", "xls", "xlsx"} {
		result, err := plan.Encode(t.Context(), format, value)
		require.NoError(t, err)
		var cells []string
		for _, r := range scalarExportRows(t, result, format) {
			cells = append(cells, r...)
		}
		for _, cell := range expected[0] {
			require.Contains(t, cells, string(cell))
		}
	}
	value[0].Map["a"] = opaqueExportEnum{State: "UNKNOWN"}
	for _, format := range []string{"csv", "xls", "xlsx"} {
		result, err := plan.Encode(t.Context(), format, value)
		require.Error(t, err)
		require.Empty(t, result.Data)
	}
	// A partial selection inside a map either fails at native selection compile
	// or at the same indexed presenter; it must never encode a wider map.
	filter, err := structjson.NewFieldFilter(reflect.TypeOf(value), []structjson.FieldSelection{{Path: []string{"Map"}, Fields: []string{"State"}}})
	if err == nil {
		for _, format := range []string{"csv", "xls", "xlsx"} {
			_, _, err := plan.selectedPresentationContext(t.Context(), value, filter, plan.formatPresentation)
			require.Error(t, err, format)
		}
	}
}
func BenchmarkOpaqueExport(b *testing.B) {
	value := opaqueExportEnvelope{Data: make([]opaqueExportRow, 100), Errors: []any{"outside-csv"}}
	for i := range value.Data {
		value.Data[i] = opaqueExportRow{ID: i, Errors: []any{nil, map[string]any{"label": "雪", "values": []any{1, true, "text"}}}}
	}
	input := CompileInput{Type: reflect.TypeOf(value), DataField: "Data", Component: &spec.Component{Settings: &spec.Settings{CaseFormat: "lc"}}}
	b.Run("compile", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			plan, err := (Compiler{}).Compile(input)
			if err != nil || plan.formatError != nil {
				b.Fatalf("%v / %v", err, plan.formatError)
			}
		}
	})
	for _, format := range []string{"csv", "xls", "xlsx"} {
		b.Run(format+"/cold", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				plan, err := (Compiler{}).Compile(input)
				if err != nil {
					b.Fatal(err)
				}
				if _, err = plan.Encode(context.Background(), format, value); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(format+"/warm", func(b *testing.B) {
			plan, err := (Compiler{}).Compile(input)
			if err != nil {
				b.Fatal(err)
			}
			if _, err = plan.Encode(context.Background(), format, value); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err = plan.Encode(context.Background(), format, value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestOpaqueEnvelopeNilEmptyAndHidden(t *testing.T) {
	for _, errors := range [][]any{nil, {}, {nil, "雪", map[string]any{"typed": opaqueExportEnum{State: "On"}}}} {
		value := &opaqueExportEnvelope{Data: []opaqueExportRow{{ID: 7}}, Errors: errors}
		plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeOf(value), DataField: "Data"})
		require.NoError(t, err)
		native, err := json.Marshal(errors)
		require.NoError(t, err)
		for _, format := range []string{"xls", "xlsx"} {
			result, err := plan.Encode(t.Context(), format, value)
			require.NoError(t, err)
			var cells []string
			for _, r := range scalarExportRows(t, result, format) {
				cells = append(cells, r...)
			}
			require.Contains(t, cells, string(native))
			require.Contains(t, cells, "Errors")
		}
		cycle := map[string]any{}
		cycle["self"] = cycle
		value.Errors = []any{cycle}
		result, err := plan.Encode(t.Context(), "csv", value)
		require.NoError(t, err)
		require.NotEmpty(t, result.Data)
		// Contract hides every Errors field before invoking a custom or cyclic owner.
		hidden, err := plan.WithRepresentation(recursiveExportMetadata(t, plan.typeOf, "Errors"))
		require.NoError(t, err)
		for _, format := range []string{"csv", "xls", "xlsx"} {
			result, err := hidden.Encode(t.Context(), format, value)
			require.NoError(t, err)
			require.NotEmpty(t, result.Data)
		}
	}
}

// New export carriers must not publish fields the native JSON owner hides.
func TestOpaqueExportStaticVisibility(t *testing.T) {
	type row struct {
		ID     int `json:"id"`
		Hidden any `json:"-"`
	}
	type envelope struct {
		Data     []row `json:"data"`
		Hidden   any   `json:"-"`
		Logger   any   `json:"-" internal:"true" mcp:"-"`
		Internal any   `internal:"true"`
		MCP      any   `mcp:"-"`
		Ignored  any   `format:"ignore=true"`
	}
	cycle := map[string]any{}
	cycle["cycle"] = cycle
	value := envelope{Data: []row{{ID: 1, Hidden: cycle}}, Hidden: cycle, Logger: map[string]any{"value": "LOGGER_SECRET"}, Internal: "INTERNAL_SECRET", MCP: "MCP_SECRET", Ignored: "FORMAT_SECRET"}
	for _, transformed := range []bool{false, true} {
		for _, mcp := range []bool{false, true} {
			t.Run(fmt.Sprintf("transformed=%t/mcp=%t", transformed, mcp), func(t *testing.T) {
				input := CompileInput{Type: reflect.TypeOf(value), DataField: "Data"}
				if transformed {
					input.Component = &spec.Component{Settings: &spec.Settings{CaseFormat: "lc"}}
				}
				plan, err := (Compiler{}).Compile(input)
				require.NoError(t, err)
				if mcp {
					plan, err = plan.WithMCPVisibility()
					require.NoError(t, err)
				}
				for _, format := range []string{"json", "csv", "xls", "xlsx"} {
					encoded, err := plan.Encode(t.Context(), format, value)
					require.NoError(t, err, "%s", format)
					text := string(encoded.Data)
					if format != "json" {
						var flat []string
						for _, r := range scalarExportRows(t, encoded, format) {
							flat = append(flat, r...)
						}
						text = strings.Join(flat, "|")
					}
					require.NotContains(t, text, "LOGGER_SECRET")
					require.NotContains(t, text, "cycle")
					if format != "csv" {
						require.Equal(t, !transformed, strings.Contains(text, "INTERNAL_SECRET"), "native internal semantics %s", format)
						require.Equal(t, format == "json" && !transformed, strings.Contains(text, "FORMAT_SECRET"), "native format semantics %s", format)
						require.Equal(t, !mcp, strings.Contains(text, "MCP_SECRET"), "native MCP visibility %s", format)
					}
				}
				ctx := recursiveSelection(t, value, structjson.FieldSelection{Path: []string{"Data"}, Fields: []string{"ID"}})
				for _, format := range []string{"csv", "xls", "xlsx"} {
					encoded, err := plan.Encode(ctx, format, value)
					require.NoError(t, err)
					for _, r := range scalarExportRows(t, encoded, format) {
						require.NotContains(t, strings.Join(r, "|"), "LOGGER_SECRET")
						require.NotContains(t, strings.Join(r, "|"), "cycle")
					}
				}
			})
		}
	}
}
