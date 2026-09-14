package output

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	structjson "github.com/viant/structology/encoding/json"
	"github.com/xuri/excelize/v2"
)

func TestSingletonAndCollectionRows(t *testing.T) {
	type row struct {
		ID   int    `json:"id" csvName:"id"`
		Name string `json:"name" csvName:"name"`
	}
	type envelope struct {
		Other  []row  `json:"other"`
		Record *row   `json:"data"`
		Status string `json:"status"`
	}
	one := &spec.Component{RootView: &spec.View{Cardinality: spec.CardinalityOne}, Settings: &spec.Settings{Output: &spec.OutputSettings{Title: "Record"}}}
	record := row{7, "Alpha"}
	records := []row{record, {8, "Beta"}}
	for _, tc := range []struct {
		name                 string
		value                any
		component            *spec.Component
		holder, csv, tabular string
	}{
		{"direct value", record, one, "", "\"id\",\"name\"\n7,\"Alpha\"", `[["id","name"],[7,"Alpha"]]`},
		{"direct pointer", &record, one, "", "\"id\",\"name\"\n7,\"Alpha\"", `[["id","name"],[7,"Alpha"]]`},
		{"nil singleton", (*row)(nil), one, "", "\"id\",\"name\"", `[]`},
		{"named singleton", envelope{Other: []row{{99, "other"}}, Record: &record, Status: "ok"}, one, "Record", "\"id\",\"name\"\n7,\"Alpha\"", `{"other":[{"id":99,"name":"other"}],"data":[["id","name"],[7,"Alpha"]],"status":"ok"}`},
		{"nil named singleton", envelope{Status: "ok"}, one, "Record", "\"id\",\"name\"", `{"other":null,"data":[],"status":"ok"}`},
		{"nil envelope", (*envelope)(nil), one, "Record", "", `null`},
		{"direct slice", records, one, "", "\"id\",\"name\"\n7,\"Alpha\"\n8,\"Beta\"", `[["id","name"],[7,"Alpha"],[8,"Beta"]]`},
		{"direct array", [2]row{record, {8, "Beta"}}, nil, "", "\"id\",\"name\"\n7,\"Alpha\"\n8,\"Beta\"", `[["id","name"],[7,"Alpha"],[8,"Beta"]]`},
		{"empty array", [0]row{}, nil, "", "\"id\",\"name\"", `[]`},
		{"inferred collection envelope", struct {
			Rows   []row  `json:"data"`
			Status string `json:"status"`
		}{records, "ok"}, nil, "", "\"id\",\"name\"\n7,\"Alpha\"\n8,\"Beta\"", `{"data":[["id","name"],[7,"Alpha"],[8,"Beta"]],"status":"ok"}`},
		{"named array", struct {
			Rows [2]row `json:"data"`
		}{[2]row{record, {8, "Beta"}}}, nil, "Rows", "\"id\",\"name\"\n7,\"Alpha\"\n8,\"Beta\"", `{"data":[["id","name"],[7,"Alpha"],[8,"Beta"]]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeOf(tc.value), Component: tc.component, DataField: tc.holder})
			require.NoError(t, err)
			require.Equal(t, reflect.TypeOf(tc.value), plan.Type())
			for _, format := range []string{"csv", "tabular"} {
				result, err := plan.Encode(context.Background(), format, tc.value)
				require.NoError(t, err)
				if format == "csv" {
					require.Equal(t, tc.csv, string(result.Data))
				} else {
					require.JSONEq(t, tc.tabular, string(result.Data))
				}
			}
			wire, err := plan.Wire("csv")
			require.NoError(t, err)
			require.Equal(t, reflect.TypeFor[string](), wire.Type)
			require.Equal(t, "text/csv", wire.ContentType)
			require.False(t, wire.Binary)
			if tc.component != nil {
				require.Equal(t, `attachment; filename=Record.csv`, wire.ContentDisposition)
			}
			wire, err = plan.Wire("json")
			require.NoError(t, err)
			require.Equal(t, reflect.TypeOf(tc.value), wire.Type)
			_, err = plan.Wire("tabular")
			require.ErrorContains(t, err, "compiled wire schema projection")
		})
	}
	for _, source := range []string{"view", "body"} {
		component := &spec.Component{Parameters: []*spec.Parameter{{Name: "Record", Source: spec.BindSource{Kind: "output", Name: source}}}}
		plan, err := (Compiler{}).Compile(CompileInput{Component: component, Type: reflect.TypeFor[envelope]()})
		require.NoError(t, err)
		encoded, err := plan.Encode(context.Background(), "csv", envelope{Record: &record})
		require.NoError(t, err)
		require.Equal(t, "\"id\",\"name\"\n7,\"Alpha\"", string(encoded.Data))
	}
	// An arbitrary struct is not authoritative row output, nor may a missing
	// explicit holder fall back to an unrelated collection.
	for _, input := range []CompileInput{{Type: reflect.TypeFor[row]()}, {Type: reflect.TypeFor[envelope](), Component: one, DataField: "Missing"}} {
		plan, err := (Compiler{}).Compile(input)
		require.NoError(t, err)
		_, err = plan.Wire("csv")
		require.ErrorContains(t, err, "requires typed rows")
	}
}

func TestSingletonRelationSelectionAndExclusion(t *testing.T) {
	type child struct {
		ID     int    `json:"id" csvName:"id"`
		Name   string `json:"name" csvName:"name"`
		Secret string `json:"secret" csvName:"secret"`
	}
	type row struct {
		ID       int     `json:"id" csvName:"id"`
		Name     string  `json:"name" csvName:"name"`
		Children []child `json:"children"`
		Omitted  string  `json:"omitted"`
	}
	type envelope struct {
		Record *row   `json:"data"`
		Status string `json:"status"`
	}
	record := &row{ID: 7, Name: "parent", Children: []child{{8, "first", "private"}, {9, "second", "private"}}, Omitted: "omit"}
	for _, named := range []bool{false, true} {
		var value any = record
		holder, exclude := "", "Children.Secret"
		path := []string(nil)
		if named {
			value = &envelope{Record: record, Status: "ok"}
			holder = "Record"
			exclude = "data.children.secret"
			path = []string{"Record"}
		}
		plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeOf(value), DataField: holder, Component: &spec.Component{RootView: &spec.View{Cardinality: spec.CardinalityOne}, Settings: &spec.Settings{Output: &spec.OutputSettings{Exclude: []string{exclude}, Title: "Record"}}}})
		require.NoError(t, err)
		filter, err := structjson.NewFieldFilter(reflect.TypeOf(value), []structjson.FieldSelection{{Path: path, Fields: []string{"ID", "Children"}}})
		require.NoError(t, err)
		ctx := dexec.CaptureOutputSelection(context.Background())
		scoped, complete := dexec.ScopeOutputSelection(ctx)
		dexec.BeginOutputSelection(scoped)
		dexec.PublishOutputSelection(scoped, value, filter)
		complete(value, nil)
		for _, selected := range []bool{false, true} {
			encodeCtx := context.Background()
			if selected {
				encodeCtx = ctx
			}
			encoded, err := plan.Encode(encodeCtx, "csv", value)
			require.NoError(t, err)
			if selected {
				require.Equal(t, "\"id\",\"Children.id\",\"Children.name\"\n7,8,\"first\"\n7,9,\"second\"", string(encoded.Data))
			} else {
				require.Contains(t, string(encoded.Data), "7,\"parent\",\"omit\",8,\"first\"")
			}
			require.NotContains(t, string(encoded.Data), "private")
			require.Equal(t, `attachment; filename=Record.csv`, encoded.ContentDisposition)
			encoded, err = plan.Encode(encodeCtx, "tabular", value)
			require.NoError(t, err)
			require.Contains(t, string(encoded.Data), `"first"`)
			require.Contains(t, string(encoded.Data), `"second"`)
			require.NotContains(t, string(encoded.Data), "private")
			if selected {
				want := `[["id","Children"],[7,[["id","name"],[8,"first"],[9,"second"]]]]`
				if named {
					want = `{"data":` + want + `,"status":"ok"}`
				}
				require.JSONEq(t, want, string(encoded.Data))
				require.NotContains(t, string(encoded.Data), "parent")
				require.NotContains(t, string(encoded.Data), "omit")
			} else {
				require.Contains(t, string(encoded.Data), "parent")
			}
			if named {
				require.Contains(t, string(encoded.Data), `"status":"ok"`)
			}
		}
	}
	require.Equal(t, "private", record.Children[0].Secret)
	require.Equal(t, "parent", record.Name)
}

func TestSingletonOtherFormatShapesUnchanged(t *testing.T) {
	type row struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	type envelope struct {
		Record *row   `json:"data"`
		Status string `json:"status"`
	}
	record := &row{7, "Alpha"}
	for _, value := range []any{record, (*row)(nil), &envelope{record, "ok"}, &envelope{nil, "empty"}} {
		input := CompileInput{Type: reflect.TypeOf(value)}
		before, err := (Compiler{}).Compile(input)
		require.NoError(t, err)
		input.Component = &spec.Component{RootView: &spec.View{Cardinality: spec.CardinalityOne}}
		if reflect.TypeOf(value) == reflect.TypeFor[*envelope]() {
			input.DataField = "Record"
		}
		after, err := (Compiler{}).Compile(input)
		require.NoError(t, err)
		for _, format := range []string{"json", "xml", "xls"} {
			expected, err := before.Encode(context.Background(), format, value)
			require.NoError(t, err)
			actual, err := after.Encode(context.Background(), format, value)
			require.NoError(t, err)
			if format != "xls" || len(expected.Data) == 0 {
				require.Equal(t, expected, actual)
				continue
			}
			expectedBook, err := excelize.OpenReader(bytes.NewReader(expected.Data))
			require.NoError(t, err)
			defer expectedBook.Close()
			actualBook, err := excelize.OpenReader(bytes.NewReader(actual.Data))
			require.NoError(t, err)
			defer actualBook.Close()
			require.Equal(t, expectedBook.GetSheetList(), actualBook.GetSheetList())
			for _, sheet := range expectedBook.GetSheetList() {
				expectedRows, err := expectedBook.GetRows(sheet)
				require.NoError(t, err)
				actualRows, err := actualBook.GetRows(sheet)
				require.NoError(t, err)
				require.Equal(t, expectedRows, actualRows)
			}
		}
	}
}
