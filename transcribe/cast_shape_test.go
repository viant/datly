package transcribe

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/transcribe/compile"
	"github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/testdata/castmodel"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestCASTPreservesImportedPointerSliceShapes(t *testing.T) {
	ctx := context.Background()
	const modelPackage = "github.com/viant/datly/transcribe/testdata/castmodel"
	db := testharness.NewSQLiteHarness(t)
	require.NoError(t, db.ExecStatements(ctx,
		"CREATE TABLE shapes(scalar TEXT, pointer TEXT, values_list TEXT, pointer_list TEXT, slice_pointer TEXT)",
		"INSERT INTO shapes VALUES(NULL,NULL,NULL,NULL,NULL)"))
	for _, policy := range []generate.GenerationPolicy{generate.GenerationPolicyMerge, generate.GenerationPolicyOverwrite} {
		t.Run(string(policy), func(t *testing.T) {
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "github.com/viant/datly/castshapefixture"}).Write(t, root)
			catalog := typecatalog.NewCatalog()
			require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[castmodel.Signals]())))
			source := &Source{
				Name: "Shapes", Scope: "github.com/viant/datly/castshapefixture/shapes", Connector: "main", Types: catalog,
				ColumnRefiner: column.New(column.Connections{"main": db.DB}),
				Text: fmt.Sprintf(`#package('shapes')
#import('signalmodel','%s')
#setting($_ = $route('/shapes','GET'))
SELECT r.scalar, r.pointer, r.values_list, r.pointer_list, r.slice_pointer,
 CAST(r.scalar AS signalmodel.Signals),
 CAST(r.pointer AS *signalmodel.Signals),
 CAST(r.values_list AS []signalmodel.Signals),
 CAST(r.pointer_list AS []*signalmodel.Signals),
 CAST(r.slice_pointer AS *[]signalmodel.Signals),
 tag(r.pointer_list,'json:"signals" sqlx:"-"')
FROM shapes r`, modelPackage),
			}
			compiled, err := NewCompiler().Compile(ctx, source)
			require.NoError(t, err)
			generator := Generator{Operation: "get", GenerationPolicy: policy}
			// Start with value slices, as emitted by the old generator, and prove
			// that exact CAST authority repairs those fields during regeneration.
			oldSource := *source
			oldSource.Text = strings.NewReplacer("AS []*signalmodel.Signals", "AS []signalmodel.Signals",
				"AS *[]signalmodel.Signals", "AS []signalmodel.Signals").Replace(source.Text)
			_, err = generator.Generate(ctx, GenerationRequest{Source: &oldSource, Destination: root})
			require.NoError(t, err)
			var previous []byte
			for attempt := 0; attempt < 2; attempt++ {
				generated, err := generator.Generate(ctx, GenerationRequest{Compiled: compiled, Destination: root})
				require.NoError(t, err)
				view := generated.Result.Plan.Views[0]
				for _, tc := range []struct{ name, typ string }{
					{"Scalar", "signalmodel.Signals"}, {"Pointer", "*signalmodel.Signals"},
					{"ValuesList", "[]signalmodel.Signals"}, {"PointerList", "[]*signalmodel.Signals"},
					{"SlicePointer", "*[]signalmodel.Signals"},
				} {
					field, found := view.Field(tc.name)
					require.True(t, found, tc.name)
					require.Equal(t, tc.typ, field.Type)
					require.True(t, field.ExplicitType)
					if tc.name == "PointerList" {
						require.Contains(t, field.Tag, `json:"signals"`)
						require.Contains(t, field.Tag, `sqlx:"-"`)
					}
				}
				content, err := os.ReadFile(filepath.Join(root, "shapes", view.Destination))
				require.NoError(t, err)
				if attempt > 0 {
					require.Equal(t, string(previous), string(content))
				}
				previous = content
				consumer := strings.ReplaceAll(castShapeConsumer, "CAST_ROW", view.Name)
				require.NoError(t, os.WriteFile(filepath.Join(root, "shapes", "shape_test.go"), []byte(consumer), 0600))
				command := exec.CommandContext(ctx, "go", "test", "-mod=mod", "-count=1", "./...")
				command.Dir = root
				out, err := command.CombinedOutput()
				require.NoError(t, err, "%s", out)
			}
		})
	}
}

func TestCASTRejectsDifferentPointerSliceShapes(t *testing.T) {
	_, err := compile.NewReader().Compile(compile.ReadInput{
		View: &spec.View{Name: "Shapes", Source: &spec.ViewSource{}},
		SQL:  "SELECT r.value, CAST(r.value AS []*int), CAST(r.value AS *[]int) FROM shapes r",
	})
	require.ErrorContains(t, err, "conflicts with another CAST")
}

const castShapeConsumer = `package shapes
import (
 "encoding/json"
 "reflect"
 "testing"
 signalmodel "github.com/viant/datly/transcribe/testdata/castmodel"
)
var (
 _ signalmodel.Signals = CAST_ROW{}.Scalar
 _ *signalmodel.Signals = CAST_ROW{}.Pointer
 _ []signalmodel.Signals = CAST_ROW{}.ValuesList
 _ []*signalmodel.Signals = CAST_ROW{}.PointerList
 _ *[]signalmodel.Signals = CAST_ROW{}.SlicePointer
)
func buildSignals() []*signalmodel.Signals { return []*signalmodel.Signals{nil,{Label:"kept"}} }
func TestExactShapes(t *testing.T) {
 values:=[]signalmodel.Signals{{Label:"value"}}
 row:=CAST_ROW{PointerList:buildSignals(),SlicePointer:&values}
 if row.PointerList[0]!=nil||row.PointerList[1].Label!="kept"||(*row.SlicePointer)[0].Label!="value" {
  t.Fatal("authored shape or nil element lost")
 }
 fields:=[]struct{name string;typ reflect.Type}{
  {"Scalar",reflect.TypeFor[signalmodel.Signals]()},
  {"Pointer",reflect.TypeFor[*signalmodel.Signals]()},
  {"ValuesList",reflect.TypeFor[[]signalmodel.Signals]()},
  {"PointerList",reflect.TypeFor[[]*signalmodel.Signals]()},
  {"SlicePointer",reflect.TypeFor[*[]signalmodel.Signals]()},
 }
 for _,want:=range fields {
  field,ok:=reflect.TypeFor[CAST_ROW]().FieldByName(want.name)
  if !ok||field.Type!=want.typ {t.Fatalf("%s: %v != %v",want.name,field.Type,want.typ)}
 }
 data,err:=json.Marshal(row);if err!=nil {t.Fatal(err)}
 var restored CAST_ROW
 if err=json.Unmarshal(data,&restored);err!=nil {t.Fatal(err)}
 if len(restored.PointerList)!=2||restored.PointerList[0]!=nil||restored.PointerList[1].Label!="kept" {
  t.Fatalf("nil slice element lost: %s",data)
 }
}
`
