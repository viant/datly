package generate

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/runtime/output"
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	structjson "github.com/viant/structology/encoding/json"
)

func TestInferredJSONTagCollisions(t *testing.T) {
	for _, tc := range []struct {
		name   string
		fields []Field
		want   []string
		err    string
	}{
		{
			name: "numeric siblings retain original options",
			fields: []Field{
				{Name: "SampleSeen_1Day", Tag: `sqlx:"sample_seen_1_day" json:",omitempty,string"`},
				{Name: "SampleSeen_7Day", Tag: `sqlx:"sample_seen_7_day"`},
				{Name: "CandidateId", Tag: `sqlx:"candidate_id" json:",omitempty"`},
			},
			want: []string{"sampleSeen1Day,omitempty,string", "sampleSeen7Day", "candidateId,omitempty"},
		},
		{
			name: "colliding siblings retain original options",
			fields: []Field{
				{Name: "SampleSeenDay", Tag: `sqlx:"seen_day" json:",omitempty,string"`},
				{Name: "SampleSeen_Day", Tag: `sqlx:"other_day"`},
			},
			want: []string{",omitempty,string", ""},
		},
		{
			name: "explicit name wins without being rewritten",
			fields: []Field{
				{Name: "SampleSeen_1Day", Tag: `json:"sampleSeen_Day,string"`},
				{Name: "SampleSeen_7Day"},
				{Name: "CandidateId", Tag: `json:"publicId,omitempty"`},
			},
			want: []string{"sampleSeen_Day,string", "sampleSeen7Day", "publicId,omitempty"},
		},
		{
			name: "explicit collision restores only inferred name",
			fields: []Field{
				{Name: "SampleSeenDay", Tag: `json:"sampleSeenDay,string"`},
				{Name: "SampleSeen_Day"},
			},
			want: []string{"sampleSeenDay,string", ""},
		},
		{
			name: "relation holder participates",
			fields: []Field{
				{Name: "SampleSeenDay"},
				{Name: "SampleSeen_Day", Tag: `view:"samples"`, RelationHolder: true},
				{Name: "OtherRelation", Tag: `view:"other"`, RelationHolder: true},
			},
			want: []string{"", "", "otherRelation"},
		},
		{
			name: "excluded fields do not conflict",
			fields: []Field{
				{Name: "SampleSeen_1Day", Tag: `json:"-"`},
				{Name: "SampleSeen_7Day"},
				{Name: "Hidden", Tag: `json:"-"`},
			},
			want: []string{"-", "sampleSeen7Day", "-"},
		},
		{
			name: "explicit conflict is reported",
			fields: []Field{
				{Name: "Day", Tag: `json:"seen"`},
				{Name: "Week", Tag: `json:"seen,omitempty"`},
			},
			err: `JSON name "seen" conflicts between generated fields "Day" and "Week"`,
		},
		{
			name: "restored name still conflicts",
			fields: []Field{
				{Name: "SampleSeenDay"},
				{Name: "SampleSeen_Day"},
				{Name: "Other", Tag: `json:"SampleSeenDay"`},
			},
			err: `JSON name "SampleSeenDay" conflicts`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := append([]Field(nil), tc.fields...)
			err := applyInferredJSONTags(&Plan{Settings: dtag.Settings{CaseFormat: "lc"}}, tc.fields)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			for i, field := range tc.fields {
				require.Equal(t, tc.want[i], reflect.StructTag(field.Tag).Get("json"))
				require.Equal(t, withoutStructTags(original[i].Tag, "json"), withoutStructTags(field.Tag, "json"))
				if tc.want[i] == reflect.StructTag(original[i].Tag).Get("json") {
					require.Equal(t, original[i].Tag, field.Tag)
				}
			}
		})
	}
}

func numericJSONComponent() *spec.Component {
	return &spec.Component{Name: "Signals", Settings: &spec.Settings{CaseFormat: "lc"}, RootView: &spec.View{Name: "signals", Columns: []*spec.Column{
		{Name: "sample_seen_1_day", Type: spec.TypeRef{Name: "int", Pointer: true}},
		{Name: "sample_seen_7_day", Type: spec.TypeRef{Name: "int", Pointer: true}},
		{Name: "candidate_id", Type: spec.TypeRef{Name: "string"}},
	}}}
}

func TestGeneratedNumericJSONPreservesOutputProjection(t *testing.T) {
	component := numericJSONComponent()
	plan := testPlan(t, component)
	require.Len(t, plan.Views, 1)
	fields := plan.Views[0].Fields
	require.Len(t, fields, 3)
	require.Equal(t, "SampleSeen1Day", fields[0].Name)
	require.Equal(t, "SampleSeen7Day", fields[1].Name)
	require.Equal(t, "sample_seen_1_day", reflect.StructTag(fields[0].Tag).Get("sqlx"))
	require.Equal(t, "sample_seen_7_day", reflect.StructTag(fields[1].Tag).Get("sqlx"))
	require.Equal(t, "sampleSeen1Day", reflect.StructTag(fields[0].Tag).Get("json"))
	require.Equal(t, "sampleSeen7Day", reflect.StructTag(fields[1].Tag).Get("json"))
	require.Equal(t, "candidateId", reflect.StructTag(fields[2].Tag).Get("json"))
	require.Equal(t, "*int", fields[0].Type)
	require.Equal(t, "*int", fields[1].Type)

	valueFor := func(fields []Field) any {
		var rowFields []reflect.StructField
		for _, field := range fields {
			typ := reflect.TypeFor[*int]()
			if field.Name == "CandidateId" {
				typ = reflect.TypeFor[string]()
			}
			rowFields = append(rowFields, reflect.StructField{Name: field.Name, Type: typ, Tag: reflect.StructTag(field.Tag)})
		}
		rowType := reflect.StructOf(rowFields)
		value := reflect.New(reflect.StructOf([]reflect.StructField{{Name: "Data", Type: reflect.SliceOf(rowType)}}))
		rows := reflect.MakeSlice(reflect.SliceOf(rowType), 1, 1)
		day, week := 11, 77
		rows.Index(0).Field(0).Set(reflect.ValueOf(&day))
		rows.Index(0).Field(1).Set(reflect.ValueOf(&week))
		rows.Index(0).Field(2).SetString("public")
		value.Elem().Field(0).Set(rows)
		return value.Interface()
	}
	value := valueFor(fields)
	encoder, err := (output.Compiler{}).Compile(output.CompileInput{Type: reflect.TypeOf(value), Component: component})
	require.NoError(t, err)
	for _, tc := range []struct{ field, want string }{
		{"SampleSeen1Day", `{"data":[{"sampleSeen1Day":11}]}`},
		{"SampleSeen7Day", `{"data":[{"sampleSeen7Day":77}]}`},
		{"CandidateId", `{"data":[{"candidateId":"public"}]}`},
	} {
		t.Run(tc.field, func(t *testing.T) {
			filter, err := structjson.NewFieldFilter(reflect.TypeOf(value), []structjson.FieldSelection{{Path: []string{"Data"}, Fields: []string{tc.field}}})
			require.NoError(t, err)
			ctx := dexec.CaptureOutputSelection(context.Background())
			scoped, complete := dexec.ScopeOutputSelection(ctx)
			dexec.BeginOutputSelection(scoped)
			dexec.PublishOutputSelection(scoped, value, filter)
			complete(value, nil)
			encoded, err := encoder.Encode(ctx, "json", value)
			require.NoError(t, err)
			require.JSONEq(t, tc.want, string(encoded.Data))
		})
	}
	legacy := append([]Field(nil), fields...)
	for i := range legacy {
		legacy[i].Tag = withoutStructTags(legacy[i].Tag, "json")
	}
	legacyValue := valueFor(legacy)
	legacyEncoder, err := (output.Compiler{}).Compile(output.CompileInput{Type: reflect.TypeOf(legacyValue), Component: component})
	require.NoError(t, err)
	want, err := legacyEncoder.Encode(context.Background(), "json", legacyValue)
	require.NoError(t, err)
	actual, err := encoder.Encode(context.Background(), "json", value)
	require.NoError(t, err)
	// Explicit inferred tags and runtime formatting must agree on full output.
	require.Equal(t, string(want.Data), string(actual.Data))
	require.JSONEq(t, `{"data":[{"sampleSeen1Day":11,"sampleSeen7Day":77,"candidateId":"public"}]}`, string(actual.Data))
}

func TestGeneratedJSONCollisionRegenerationBuilds(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	dir := filepath.Join(root, "signals")
	generator, err := newTestGenerator(numericJSONComponent(), nil)
	require.NoError(t, err)
	// Seed the package with the previous per-field inference behavior, then
	// verify normal regeneration replaces the conflicting generated tags.
	previous := testPlan(t, numericJSONComponent())
	for i, name := range []string{"SampleSeen_1Day", "SampleSeen_7Day"} {
		field := &previous.Views[0].Fields[i]
		field.Name = name
		field.Tag = appendStructTag(withoutStructTags(field.Tag, "json"), "json", "sampleSeen_Day")
	}
	_, err = EmitScaffold(dir, previous)
	require.NoError(t, err)
	old, err := os.ReadFile(filepath.Join(dir, previous.Views[0].Destination))
	require.NoError(t, err)
	require.Contains(t, string(old), `json:"sampleSeen_Day"`)
	first, err := generator.Generate(dir)
	require.NoError(t, err)
	path := filepath.Join(dir, first.Plan.Views[0].Destination)
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NotContains(t, string(before), `json:"sampleSeen_Day`)
	require.NotContains(t, string(before), "SampleSeen_1Day")
	require.NotContains(t, string(before), "SampleSeen_7Day")
	require.Contains(t, string(before), `json:"sampleSeen1Day"`)
	require.Contains(t, string(before), `json:"sampleSeen7Day"`)
	require.Contains(t, string(before), `json:"candidateId"`)
	_, err = generator.Generate(dir)
	require.NoError(t, err)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, string(before), string(after))
	cmd := exec.Command("go", "test", "-mod=mod", "./...")
	cmd.Dir = root
	result, err := cmd.CombinedOutput()
	require.NoError(t, err, string(result))
}
