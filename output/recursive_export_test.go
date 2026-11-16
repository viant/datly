package output

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	structjson "github.com/viant/structology/encoding/json"
	"github.com/viant/tagly/choice"
	xshape "github.com/viant/x/shape"
)

type recursiveExportNode struct {
	Label    string                 `json:"label"`
	State    int                    `json:"state" choice:"0:Off" choice:"1:On" enumStyle:"literal"`
	When     time.Time              `json:"when"`
	Children []*recursiveExportNode `json:"children"`
	Internal string                 `json:"-" choice:"1:One" enumStyle:"literal" enumOutputUnknown:"reject"`
}
type recursiveExportNodes []*recursiveExportNode
type recursiveExportRow struct {
	ID     int                   `json:"id"`
	Tree   *recursiveExportNodes `json:"dimensions" csvName:"Dimensions" xls:"name=Dimensions"`
	Secret string                `json:"secret"`
}
type recursiveExportOutput struct {
	Data []*recursiveExportRow `json:"data"`
}

func recursiveExportFixture() *recursiveExportOutput {
	shared := &recursiveExportNode{Label: "shared, \"雪\"\nleaf", State: 1, When: time.Date(2021, 2, 3, 4, 5, 6, 0, time.UTC), Children: []*recursiveExportNode{}}
	nodes := recursiveExportNodes{nil, {Label: "root", Children: []*recursiveExportNode{nil, {Label: "branch", Children: []*recursiveExportNode{shared}}, shared}}}
	empty := recursiveExportNodes{}
	return &recursiveExportOutput{Data: []*recursiveExportRow{{ID: 7, Tree: &nodes, Secret: "SECRET"}, {ID: 8}, {ID: 9, Tree: &empty}}}
}
func recursiveExportPlan(t testing.TB, transformed bool) *Plan {
	t.Helper()
	input := CompileInput{Type: reflect.TypeFor[recursiveExportOutput](), DataField: "Data"}
	if transformed {
		input.Component = &spec.Component{Settings: &spec.Settings{CaseFormat: "lc", DateFormat: "yyyy-MM-dd", Output: &spec.OutputSettings{NilSlicePolicy: "empty_array"}}}
	}
	plan, err := (Compiler{}).Compile(input)
	require.NoError(t, err)
	require.NoError(t, plan.formatError)
	require.NotNil(t, plan.formatPresentation)
	return plan
}
func recursiveExportMetadata(t testing.TB, typ reflect.Type, hidden string) *xshape.Representation {
	t.Helper()
	walk, err := xshape.CompileRewrite(typ)
	require.NoError(t, err)
	metadata, err := walk.RewriteRepresentation(func(_ string, _ reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		if f.Name != hidden {
			return nil, nil
		}
		copy := *f
		copy.Tag = `json:"-"`
		return &copy, nil
	})
	require.NoError(t, err)
	return metadata
}
func recursiveSelection(t testing.TB, value any, selections ...structjson.FieldSelection) context.Context {
	t.Helper()
	filter, err := structjson.NewFieldFilter(reflect.TypeOf(value), selections)
	require.NoError(t, err)
	ctx := dexec.CaptureOutputSelection(context.Background())
	scope, finish := dexec.ScopeOutputSelection(ctx)
	dexec.BeginOutputSelection(scope)
	dexec.PublishOutputSelection(scope, value, filter)
	finish(value, nil)
	return ctx
}
func recursiveCells(t *testing.T, result Result, format string) []any {
	t.Helper()
	var values []any
	for _, row := range scalarExportRows(t, result, format) {
		for _, cell := range row {
			if strings.HasPrefix(cell, "[") || cell == "null" {
				var value any
				require.NoError(t, json.Unmarshal([]byte(cell), &value), cell)
				values = append(values, value)
			}
		}
	}
	return values
}
func TestRecursiveExportCellsMatchNativeJSON(t *testing.T) {
	for _, transformed := range []bool{false, true} {
		t.Run(fmt.Sprint(transformed), func(t *testing.T) {
			base := recursiveExportPlan(t, transformed)
			value := recursiveExportFixture()
			before, err := json.Marshal(value)
			require.NoError(t, err)
			hidden, err := base.WithRepresentation(recursiveExportMetadata(t, base.typeOf, "Secret"))
			require.NoError(t, err)
			contexts := []context.Context{t.Context(), recursiveSelection(t, value, structjson.FieldSelection{Path: []string{"Data"}, Fields: []string{"ID", "Tree"}})}
			for _, ctx := range contexts {
				body, err := hidden.Encode(ctx, "json", value)
				require.NoError(t, err)
				var document map[string][]map[string]any
				require.NoError(t, json.Unmarshal(body.Data, &document))
				expected := []any{document["data"][0]["dimensions"], document["data"][1]["dimensions"], document["data"][2]["dimensions"]}
				for _, format := range []string{"csv", "xls", "xlsx"} {
					t.Run(format, func(t *testing.T) {
						result, err := hidden.Encode(ctx, format, value)
						require.NoError(t, err)
						require.NotEmpty(t, result.Data)
						require.Equal(t, expected, recursiveCells(t, result, format))
						text := fmtRows(scalarExportRows(t, result, format))
						require.NotContains(t, text, "SECRET")
						require.NotContains(t, text, "Internal")
						require.Contains(t, text, "Dimensions")
					})
				}
			}
			after, err := json.Marshal(value)
			require.NoError(t, err)
			require.Equal(t, before, after)
			root := (*value.Data[0].Tree)[1]
			require.Same(t, root.Children[1].Children[0], root.Children[2])
			require.Nil(t, root.Children[0])
		})
	}
}
func TestRecursiveExportPlainNoEnumGraph(t *testing.T) {
	type node struct {
		Label    string  `json:"label"`
		Children []*node `json:"children"`
	}
	type row struct {
		ID   int
		Tree []*node
	}
	value := []row{{ID: 5, Tree: []*node{{Label: "plain", Children: []*node{{Label: "leaf"}}}}}}
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeOf(value)})
	require.NoError(t, err)
	require.NoError(t, plan.formatError)
	for _, format := range []string{"csv", "xls", "xlsx"} {
		result, err := plan.Encode(t.Context(), format, value)
		require.NoError(t, err)
		cells := recursiveCells(t, result, format)
		require.Len(t, cells, 1)
		require.Contains(t, fmtRows(scalarExportRows(t, result, format)), "leaf")
	}
}
func TestRecursiveExportPartialSelectionFailsClosed(t *testing.T) {
	plan := recursiveExportPlan(t, true)
	value := recursiveExportFixture()
	ctx := recursiveSelection(t, value, structjson.FieldSelection{Path: []string{"Data", "Tree"}, Fields: []string{"Label"}})
	for _, format := range []string{"csv", "xls", "xlsx"} {
		result, err := plan.Encode(ctx, format, value)
		require.ErrorContains(t, err, "partial recursive output selection")
		require.Empty(t, result.Data)
	}
	// Whole-field omission happens before encoding, including an invalid cycle.
	root := (*value.Data[0].Tree)[1]
	root.Children = []*recursiveExportNode{root}
	ctx = recursiveSelection(t, value, structjson.FieldSelection{Path: []string{"Data"}, Fields: []string{"ID"}})
	hidden, err := plan.WithRepresentation(recursiveExportMetadata(t, plan.typeOf, "Tree"))
	require.NoError(t, err)
	for _, format := range []string{"csv", "xls", "xlsx"} {
		result, err := plan.Encode(ctx, format, value)
		require.NoError(t, err)
		require.NotContains(t, fmtRows(scalarExportRows(t, result, format)), "root")
		result, err = hidden.Encode(t.Context(), format, value)
		require.NoError(t, err)
		require.NotContains(t, fmtRows(scalarExportRows(t, result, format)), "root")
	}
}
func TestRecursiveExportNativeErrorsPreserveCauses(t *testing.T) {
	for _, transformed := range []bool{false, true} {
		t.Run(fmt.Sprint(transformed), func(t *testing.T) {
			plan := recursiveExportPlan(t, transformed)
			value := recursiveExportFixture()
			root := (*value.Data[0].Tree)[1]
			root.Children = []*recursiveExportNode{root}
			for _, format := range []string{"csv", "xls", "xlsx"} {
				result, err := plan.Encode(t.Context(), format, value)
				var cycle *json.UnsupportedValueError
				require.ErrorAs(t, err, &cycle)
				require.Empty(t, result.Data)
			}
			root.Children = nil
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			for _, format := range []string{"csv", "xls", "xlsx"} {
				result, err := plan.Encode(ctx, format, value)
				require.ErrorIs(t, err, context.Canceled)
				require.Empty(t, result.Data)
			}
			deadline, cancelDeadline := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
			defer cancelDeadline()
			result, err := plan.Encode(deadline, "csv", value)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Empty(t, result.Data)
		})
	}
	// The enum owner retains its typed error through cell and export boundaries.
	type node struct {
		State    string  `json:"state" choice:"1:On" enumStyle:"literal" enumOutputUnknown:"reject"`
		Children []*node `json:"children"`
	}
	type row struct{ Tree []*node }
	value := []row{{Tree: []*node{{State: "UNKNOWN"}}}}
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeOf(value)})
	require.NoError(t, err)
	for _, format := range []string{"csv", "xls", "xlsx"} {
		result, err := plan.Encode(t.Context(), format, value)
		require.ErrorIs(t, err, choice.ErrUnknownOutput)
		require.Empty(t, result.Data)
	}
}
func TestRecursiveExportOppositeVariantsConcurrent(t *testing.T) {
	base := recursiveExportPlan(t, true)
	value := recursiveExportFixture()
	hidden, err := base.WithRepresentation(recursiveExportMetadata(t, base.typeOf, "Secret"))
	require.NoError(t, err)
	prepared := base.formatPresentation
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			plan := base
			if i%2 == 1 {
				plan = hidden
			}
			for j := 0; j < 4; j++ {
				result, err := plan.Encode(context.Background(), "csv", value)
				if err != nil {
					t.Errorf("encode: %v", err)
					return
				}
				if strings.Contains(string(result.Data), "SECRET") != (i%2 == 0) {
					t.Errorf("crossed variant: %d", i)
				}
			}
		}(i)
	}
	wg.Wait()
	require.Same(t, prepared, base.formatPresentation)
}

var recursiveCodecFailure = fmt.Errorf("recursive custom codec failure")

type recursiveFailScalar string

func (recursiveFailScalar) MarshalJSON() ([]byte, error) { return nil, recursiveCodecFailure }
func TestRecursiveExportCustomCauseAndRecovery(t *testing.T) {
	type node struct {
		Label    string              `json:"label"`
		Custom   recursiveFailScalar `json:"custom"`
		Children []*node             `json:"children"`
	}
	type row struct{ Tree []*node }
	value := []row{{Tree: []*node{{Label: "root"}}}}
	for _, transformed := range []bool{false, true} {
		input := CompileInput{Type: reflect.TypeOf(value)}
		if transformed {
			input.Component = &spec.Component{Settings: &spec.Settings{CaseFormat: "lc"}}
		}
		plan, err := (Compiler{}).Compile(input)
		require.NoError(t, err)
		for _, format := range []string{"csv", "xls", "xlsx"} {
			result, err := plan.Encode(t.Context(), format, value)
			require.ErrorIs(t, err, recursiveCodecFailure)
			require.Empty(t, result.Data)
		}
		// A failed operation cannot poison the immutable prepared encoder's next call.
		value[0].Tree = nil
		result, err := plan.Encode(t.Context(), "csv", value)
		require.NoError(t, err)
		require.NotEmpty(t, result.Data)
		value[0].Tree = []*node{{Label: "root"}}
	}
}
func TestRecursiveExportDepthAndExclusionBoundaries(t *testing.T) {
	for _, transformed := range []bool{false, true} {
		plan := recursiveExportPlan(t, transformed)
		value := recursiveExportFixture()
		root := (*value.Data[0].Tree)[1]
		tail := root
		for i := 0; i < 10050; i++ {
			next := &recursiveExportNode{}
			tail.Children = []*recursiveExportNode{next}
			tail = next
		}
		for _, format := range []string{"csv", "xls", "xlsx"} {
			result, err := plan.Encode(t.Context(), format, value)
			require.ErrorContains(t, err, "maximum nesting depth")
			require.Empty(t, result.Data)
		}
		// The native delegate recovers after a depth failure without stale ancestors.
		root.Children = nil
		result, err := plan.Encode(t.Context(), "csv", value)
		require.NoError(t, err)
		require.NotEmpty(t, result.Data)
	}
	input := CompileInput{Type: reflect.TypeFor[recursiveExportOutput](), DataField: "Data", Component: &spec.Component{Settings: &spec.Settings{Output: &spec.OutputSettings{Exclude: []string{"Data.Tree.Label"}}}}}
	plan, err := (Compiler{}).Compile(input)
	require.NoError(t, err)
	for _, format := range []string{"csv", "xls", "xlsx"} {
		result, err := plan.Encode(t.Context(), format, recursiveExportFixture())
		require.ErrorContains(t, err, "requires complete unchanged selection")
		require.Empty(t, result.Data)
	}
	rewrite, err := xshape.CompileRewrite(reflect.TypeFor[recursiveExportOutput]())
	require.NoError(t, err)
	_, err = rewrite.RewriteRepresentation(func(_ string, _ reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		if f.Name != "Label" {
			return nil, nil
		}
		copy := *f
		copy.Tag = `json:"-"`
		return &copy, nil
	})
	require.Error(t, err, "native x/shape must continue rejecting recursive metadata edits")
}

func BenchmarkRecursiveExport(b *testing.B) {
	for _, rows := range []int{1, 100, 1000} {
		for _, depth := range []int{1, 10} {
			b.Run(fmt.Sprintf("rows%d/depth%d", rows, depth), func(b *testing.B) {
				type node struct {
					Label    string  `json:"label"`
					Children []*node `json:"children"`
				}
				type row struct {
					ID   int
					Tree []*node
				}
				root := &node{Label: "雪"}
				tail := root
				for i := 1; i < depth; i++ {
					next := &node{Label: "leaf"}
					tail.Children = []*node{next}
					tail = next
				}
				value := make([]row, rows)
				for i := range value {
					value[i] = row{ID: i, Tree: []*node{root}}
				}
				input := CompileInput{Type: reflect.TypeOf(value), Component: &spec.Component{Settings: &spec.Settings{CaseFormat: "lc"}}}
				b.Run("compile", func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						plan, err := (Compiler{}).Compile(input)
						if err != nil || plan.formatError != nil {
							b.Fatalf("compile: %v / %v", err, plan.formatError)
						}
					}
				})
				plan, err := (Compiler{}).Compile(input)
				if err != nil {
					b.Fatal(err)
				}
				for _, format := range []string{"csv", "xlsx"} {
					b.Run(format+"/warm", func(b *testing.B) {
						if _, err := plan.Encode(context.Background(), format, value); err != nil {
							b.Fatal(err)
						}
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							if _, err := plan.Encode(context.Background(), format, value); err != nil {
								b.Fatal(err)
							}
						}
					})
				}
			})
		}
	}
}
func BenchmarkAcyclicExportCompile(b *testing.B) {
	type row struct {
		ID    int
		Label string
	}
	input := CompileInput{Type: reflect.TypeFor[[]row]()}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		plan, err := (Compiler{}).Compile(input)
		if err != nil {
			b.Fatal(err)
		}
		if plan.formatPlan != nil {
			b.Fatal("unchanged acyclic output must retain its original plan")
		}
	}
}

func BenchmarkAcyclicExport(b *testing.B) {
	type row struct {
		ID    int
		Label string
	}
	value := []row{{ID: 7, Label: "control 雪"}}
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeOf(value)})
	if err != nil {
		b.Fatal(err)
	}
	for _, format := range []string{"csv", "xlsx"} {
		b.Run(format, func(b *testing.B) {
			if _, err := plan.Encode(context.Background(), format, value); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := plan.Encode(context.Background(), format, value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
func TestRecursiveExportAuthorityReusedForSelection(t *testing.T) {
	plan := recursiveExportPlan(t, true)
	value := recursiveExportFixture()
	// Canonical indexes: Data -> row.Tree. The cell authority belongs to the
	// immutable compiled variant, including across independently scoped selections.
	cell := plan.formatPresentation.byIndex[0].elem.elem.byIndex[1]
	require.NotNil(t, cell.jsonCell)
	require.Equal(t, reflect.TypeFor[string](), cell.target)
	original := cell.jsonCell
	calls := 0
	cell.jsonCell = func(ctx context.Context, value any) ([]byte, error) { calls++; return original(ctx, value) }
	defer func() { cell.jsonCell = original }()
	for i := 0; i < 3; i++ {
		ctx := recursiveSelection(t, value, structjson.FieldSelection{Path: []string{"Data"}, Fields: []string{"Tree"}})
		_, err := plan.Encode(ctx, "csv", value)
		require.NoError(t, err)
		require.Same(t, cell, plan.formatPresentation.byIndex[0].elem.elem.byIndex[1])
	}
	require.Equal(t, 9, calls, "one native call per actual selected field value, no eager/repeated encoding")
	ctx := recursiveSelection(t, value, structjson.FieldSelection{Path: []string{"Data"}, Fields: []string{"ID"}})
	_, err := plan.Encode(ctx, "csv", value)
	require.NoError(t, err)
	require.Equal(t, 9, calls, "whole-field omission must not invoke the JSON owner")
}

type recursiveNamedSlice []recursiveNamedSlice
type recursiveNamedMap map[string]recursiveNamedMap
type recursiveNamedInterface interface {
	Next() recursiveNamedInterface
}
type recursiveOpaqueSlice []recursiveOpaqueSlice

func (recursiveOpaqueSlice) MarshalJSON() ([]byte, error) {
	return []byte(`"opaque-recursive-owner"`), nil
}
func TestRecursiveExportNonObjectCompilationBoundaries(t *testing.T) {
	// The native active traversal tracks containers as well as structs, so a
	// legal named slice cannot overflow compilation even without an object node.
	root, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[recursiveNamedSlice]()})
	require.NoError(t, err)
	for _, format := range []string{"csv", "xls", "xlsx"} {
		result, err := root.Encode(t.Context(), format, recursiveNamedSlice{})
		require.ErrorContains(t, err, "recursive effective output")
		require.Empty(t, result.Data)
	}
	type row struct{ Values recursiveNamedSlice }
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[[]row]()})
	require.NoError(t, err)
	require.NoError(t, plan.formatError)
	value := []row{{Values: recursiveNamedSlice{nil, recursiveNamedSlice{recursiveNamedSlice{}}}}}
	for _, format := range []string{"csv", "xls", "xlsx"} {
		result, err := plan.Encode(t.Context(), format, value)
		require.NoError(t, err)
		require.Equal(t, []any{[]any{nil, []any{[]any{}}}}, recursiveCells(t, result, format))
	}
	// Maps and interface contracts have no static row/worksheet contract here.
	// Existing native rejection remains explicit; there is no dynamic graph scan.
	for _, typ := range []reflect.Type{reflect.TypeFor[recursiveNamedMap](), reflect.TypeFor[recursiveNamedInterface]()} {
		p, err := (Compiler{}).Compile(CompileInput{Type: typ})
		require.NoError(t, err)
		for _, format := range []string{"csv", "xls", "xlsx"} {
			_, err := p.Encode(t.Context(), format, recursiveNamedMap{"leaf": nil})
			require.Error(t, err)
		}
	}
	opaque, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[recursiveOpaqueSlice]()})
	require.NoError(t, err)
	require.Nil(t, opaque.formatError)
	body, err := opaque.Encode(t.Context(), "json", recursiveOpaqueSlice{})
	require.NoError(t, err)
	require.Equal(t, `"opaque-recursive-owner"`, string(body.Data))
	require.False(t, opaque.SupportsFormat("csv"))
}
