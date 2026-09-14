package output

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/xuri/excelize/v2"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	structjson "github.com/viant/structology/encoding/json"
)

type customJSON struct{}

func (*customJSON) Marshal(value any) ([]byte, error) {
	return json.Marshal(struct {
		Custom any `json:"custom"`
	}{value})
}

func TestOutputJSONPolicy(t *testing.T) {
	type row struct {
		FirstName string
		Created   time.Time
		Secret    string
		Empty     string
	}
	type envelope struct {
		Rows []row `json:"data"`
	}
	component := &spec.Component{Settings: &spec.Settings{CaseFormat: "lc", DateFormat: "2006-01-02", Output: &spec.OutputSettings{Exclude: []string{"Secret"}, OmitEmpty: true}}}
	plan, err := (Compiler{}).Compile(CompileInput{Component: component, Type: reflect.TypeOf(envelope{}), DataField: "Rows"})
	if err != nil {
		t.Fatal(err)
	}
	value := &envelope{Rows: []row{{FirstName: "Alice", Created: time.Date(2026, 9, 12, 0, 0, 0, 0, time.UTC), Secret: "private"}}}
	encoded, err := plan.Encode(context.Background(), "json", value)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"data":[{"firstName":"Alice","created":"2026-09-12"}]}`
	if string(encoded.Data) != want {
		t.Fatalf("JSON %s expected %s", encoded.Data, want)
	}
	if value.Rows[0].Secret != "private" {
		t.Fatal("serialization mutated source")
	}
}

func TestCustomMarshallerAuthority(t *testing.T) {
	for _, tt := range []struct {
		name   string
		lookup func(string) (reflect.Type, error)
		reject bool
	}{
		{"missing lookup", nil, true},
		{"missing type", func(string) (reflect.Type, error) { return nil, nil }, true},
		{"lookup error", func(string) (reflect.Type, error) { return nil, errors.New("missing") }, true},
		{"wrong type", func(string) (reflect.Type, error) { return reflect.TypeOf(0), nil }, true},
		{"registered", func(string) (reflect.Type, error) { return reflect.TypeOf(customJSON{}), nil }, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := (Compiler{Lookup: tt.lookup}).Compile(CompileInput{Component: &spec.Component{Settings: &spec.Settings{JSONMarshalType: "example.Custom"}}, Type: reflect.TypeOf(0)})
			if tt.reject {
				if err == nil {
					t.Fatal("expected registration error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := plan.Encode(context.Background(), "json", 7)
			if err != nil || string(encoded.Data) != `{"custom":7}` {
				t.Fatalf("custom %s error %v", encoded.Data, err)
			}
		})
	}
}

func TestOutputNilAndFailures(t *testing.T) {
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeOf(struct{ Rows []struct{ ID int } }{})})
	if err != nil {
		t.Fatal(err)
	}
	for _, format := range []string{"json", "csv", "xml", "tabular", "xls"} {
		encoded, err := plan.Encode(context.Background(), format, nil)
		if err != nil || len(encoded.Data) != 0 {
			t.Fatalf("nil %s: %+v %v", format, encoded, err)
		}
	}
	if _, err = plan.Encode(context.Background(), "json", make(chan int)); err == nil {
		t.Fatal("unsupported JSON type accepted")
	}
	if _, err = plan.Encode(context.Background(), "invalid", nil); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("format error %v", err)
	}
}

func TestWireAliasExclusionUsesCanonicalRows(t *testing.T) {
	type row struct {
		ID     int
		Secret string
	}
	type envelope struct {
		Rows []row `json:"data"`
	}
	for _, format := range []string{"csv", "tabular"} {
		plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeOf(envelope{}), Component: &spec.Component{Settings: &spec.Settings{Output: &spec.OutputSettings{Exclude: []string{"data.Secret"}}}}, DataField: "Rows"})
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := plan.Encode(context.Background(), format, &envelope{Rows: []row{{ID: 1, Secret: "private"}}})
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(encoded.Data), "private") {
			t.Fatalf("alias exclusion leaked through %s: %s", format, encoded.Data)
		}
	}
}

func TestNestedExclusionAliasesAreCompiledForEveryFormat(t *testing.T) {
	type profile struct {
		Public string `json:"display"`
		Secret string `json:"secret_alias"`
	}
	type row struct {
		ID      int
		Profile profile `json:"details"`
	}
	type envelope struct {
		Rows []row `json:"data"`
	}
	for _, path := range []string{"Rows.Profile.Secret", "data.details.secret_alias"} {
		plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeOf(envelope{}), DataField: "Rows", Component: &spec.Component{Settings: &spec.Settings{Output: &spec.OutputSettings{Exclude: []string{path}}}}})
		if err != nil {
			t.Fatal(err)
		}
		for _, format := range []string{"json", "csv", "xml", "tabular", "xls"} {
			t.Run(path+"/"+format, func(t *testing.T) {
				value := &envelope{Rows: []row{{ID: 1, Profile: profile{Public: "visible", Secret: "private"}}}}
				encoded, err := plan.Encode(context.Background(), format, value)
				if err != nil {
					t.Fatal(err)
				}
				text := string(encoded.Data)
				if format == "xls" {
					workbook, err := excelize.OpenReader(bytes.NewReader(encoded.Data))
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
					text = strings.Join(cells, "|")
				}
				if strings.Contains(text, "private") || !strings.Contains(text, "visible") {
					t.Fatalf("bad redaction: %s", text)
				}
				if value.Rows[0].Profile.Secret != "private" {
					t.Fatal("source mutated")
				}
			})
		}
	}
	if _, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeOf(envelope{}), Component: &spec.Component{Settings: &spec.Settings{Output: &spec.OutputSettings{Exclude: []string{"Rows.unknown.Secret"}}}}}); err == nil {
		t.Fatal("invalid exclusion accepted")
	}
}

func TestNativeEncoderReuseConcurrent(t *testing.T) {
	type row struct {
		ID   int
		Name string
	}
	type envelope struct{ Rows []row }
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeOf(envelope{}), DataField: "Rows"})
	if err != nil {
		t.Fatal(err)
	}
	value := &envelope{Rows: []row{{1, "stable"}}}
	for _, format := range []string{"csv", "xml"} {
		expected, err := plan.Encode(context.Background(), format, value)
		if err != nil {
			t.Fatal(err)
		}
		var workers sync.WaitGroup
		failures := make(chan error, 8)
		for i := 0; i < 8; i++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				actual, err := plan.Encode(context.Background(), format, value)
				if err != nil {
					failures <- err
					return
				}
				if !bytes.Equal(actual.Data, expected.Data) {
					failures <- errors.New("concurrent output changed")
				}
			}()
		}
		workers.Wait()
		close(failures)
		for err := range failures {
			t.Error(err)
		}
	}
	if plan.csvEncoder.encoder == nil {
		t.Fatal("CSV metadata was not retained")
	}
	count := 0
	plan.xmlEncoders.Range(func(_, _ any) bool { count++; return true })
	if count != 1 {
		t.Fatalf("XML codec cache has %d entries", count)
	}
}

func TestPromotedOutputFieldsCannotBypassExclusions(t *testing.T) {
	type Base struct {
		Secret string
		Public string
	}
	type row struct {
		Base
		Name string
	}
	type envelope struct {
		Rows []row `json:"data"`
	}
	for _, path := range []string{"Rows.Secret", "Rows.Base.Secret", "Rows.Base"} {
		plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeOf(envelope{}), DataField: "Rows", Component: &spec.Component{Settings: &spec.Settings{Output: &spec.OutputSettings{Exclude: []string{path}}}}})
		if err != nil {
			t.Fatal(err)
		}
		for _, format := range []string{"json", "csv", "tabular", "xml", "xls"} {
			t.Run(path+"/"+format, func(t *testing.T) {
				value := &envelope{Rows: []row{{Base: Base{Secret: "private", Public: "base-public"}, Name: "visible"}}}
				encoded, err := plan.Encode(context.Background(), format, value)
				if err != nil {
					t.Fatal(err)
				}
				text := string(encoded.Data)
				if format == "xls" {
					book, err := excelize.OpenReader(bytes.NewReader(encoded.Data))
					if err != nil {
						t.Fatal(err)
					}
					defer book.Close()
					var cells []string
					for _, sheet := range book.GetSheetList() {
						rows, err := book.GetRows(sheet)
						if err != nil {
							t.Fatal(err)
						}
						for _, row := range rows {
							cells = append(cells, row...)
						}
					}
					text = strings.Join(cells, "|")
				}
				if strings.Contains(text, "private") || !strings.Contains(text, "visible") {
					t.Fatalf("promoted exclusion %s: %s", format, text)
				}
				if value.Rows[0].Secret != "private" {
					t.Fatal("source mutated")
				}
			})
		}
	}
}

func TestWholeEmbeddedExclusionDoesNotExcludeEnvelopeSibling(t *testing.T) {
	type Base struct{ Secret string }
	type row struct {
		Base
		Name string
	}
	type envelope struct {
		Rows []row  `json:"data"`
		Data string `json:"other"`
	}
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeOf(envelope{}), DataField: "Rows", Component: &spec.Component{Settings: &spec.Settings{Output: &spec.OutputSettings{Exclude: []string{"Rows.Base"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := plan.Encode(context.Background(), "json", &envelope{Rows: []row{{Base: Base{Secret: "private"}, Name: "visible"}}, Data: "keep"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded.Data), "private") || !strings.Contains(string(encoded.Data), `"other":"keep"`) {
		t.Fatalf("overbroad embedded exclusion: %s", encoded.Data)
	}
}

func TestOutputSelectionCombinesConfiguredAndInvocationFilters(t *testing.T) {
	type detail struct {
		Label  string
		Secret string
		Zero   int
	}
	type row struct {
		Detail  []detail
		Omitted string
	}
	type envelope struct{ Rows []row }
	value := &envelope{Rows: []row{{Detail: []detail{{Label: "keep", Secret: "private"}}}}}
	native, err := structjson.NewFieldFilter(reflect.TypeOf(value), []structjson.FieldSelection{
		{Path: []string{"Rows"}, Fields: []string{"Detail"}},
		{Path: []string{"Rows", "Detail"}, Fields: []string{"Label", "Secret", "Zero"}},
	})
	require.NoError(t, err)
	ctx := dexec.CaptureOutputSelection(context.Background())
	scoped, complete := dexec.ScopeOutputSelection(ctx)
	dexec.BeginOutputSelection(scoped)
	dexec.PublishOutputSelection(scoped, value, native)
	complete(value, nil)
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeOf(value), DataField: "Rows", Component: &spec.Component{Settings: &spec.Settings{CaseFormat: "lc", Output: &spec.OutputSettings{Exclude: []string{"Detail.Secret"}}}}})
	require.NoError(t, err)
	encoded, err := plan.Encode(ctx, "json", value)
	require.NoError(t, err)
	require.JSONEq(t, `{"rows":[{"detail":[{"label":"keep","zero":0}]}]}`, string(encoded.Data))
	require.Equal(t, "private", value.Rows[0].Detail[0].Secret)
	_, err = plan.Encode(ctx, "xml", value)
	require.NoError(t, err)
	plan.custom = &customJSON{}
	encoded, err = plan.Encode(ctx, "json", value)
	require.NoError(t, err)
	require.Contains(t, string(encoded.Data), `"Secret":"private"`)
}

func TestOutputSelectionCompilationStopsAtFullyIncludedRecursiveBranch(t *testing.T) {
	type node struct {
		Value int
		Next  *node
	}
	type output struct {
		Rows   []node
		Secret string
	}
	value := &output{Rows: []node{{Value: 0, Next: &node{Value: 2}}}, Secret: "private"}
	filter, err := structjson.NewFieldFilter(reflect.TypeOf(value), []structjson.FieldSelection{{Fields: []string{"Rows"}}})
	require.NoError(t, err)
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeOf(value), DataField: "Rows"})
	require.NoError(t, err)
	selected, projected, err := plan.selectedPresentation(value, filter)
	require.NoError(t, err)
	rows, err := selected.rows.value(projected)
	require.NoError(t, err)
	require.Equal(t, reflect.TypeFor[[]node](), rows.Type())
	require.Equal(t, "private", value.Secret)
}

func TestOutputSelectionSharedRowTypeAcrossFormats(t *testing.T) {
	type child struct {
		ID   int
		Name string
	}
	type row struct {
		A child
		B child
	}
	type envelope struct{ Rows []row }
	value := &envelope{Rows: []row{{A: child{1, "first"}, B: child{2, "second"}}}}
	filter, err := structjson.NewFieldFilter(reflect.TypeOf(value), []structjson.FieldSelection{{Path: []string{"Rows", "A"}, Fields: []string{"Name"}}, {Path: []string{"Rows", "B"}, Fields: []string{"Name"}}})
	require.NoError(t, err)
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeOf(value), DataField: "Rows"})
	require.NoError(t, err)
	ctx := dexec.CaptureOutputSelection(context.Background())
	scoped, complete := dexec.ScopeOutputSelection(ctx)
	dexec.BeginOutputSelection(scoped)
	dexec.PublishOutputSelection(scoped, value, filter)
	complete(value, nil)
	for _, format := range []string{"csv", "xml", "tabular", "xlsx"} {
		t.Run(format, func(t *testing.T) {
			encoded, err := plan.Encode(ctx, format, value)
			require.NoError(t, err)
			if format == "xlsx" {
				book, err := excelize.OpenReader(bytes.NewReader(encoded.Data))
				require.NoError(t, err)
				defer book.Close()
				rows, err := book.GetRows(book.GetSheetList()[0])
				require.NoError(t, err)
				require.Equal(t, [][]string{{"A", "B"}, {"Name", "Name"}, {"first", "second"}}, rows)
			} else {
				require.Contains(t, string(encoded.Data), "first")
				require.Contains(t, string(encoded.Data), "second")
				require.NotContains(t, string(encoded.Data), "ID")
			}
		})
	}
	require.Equal(t, 1, value.Rows[0].A.ID)
	require.Equal(t, 2, value.Rows[0].B.ID)
}
