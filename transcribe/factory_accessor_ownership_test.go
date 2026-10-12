package transcribe

import (
	"context"
	"database/sql"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/transcribe/column"
)

func TestFactoryAuthoredAccessorOwnership(t *testing.T) {
	root, source := factoryInputShapeFixture(t)
	source.Text = strings.ReplaceAll(source.Text, "[]*Res>", "[]*Record>")
	source.Text = strings.Replace(source.Text, factoryInputProjection, `SELECT r.*,type(r,'Record'),dest(r,'record.go'),CAST(r.COUNTER AS int),CAST(r.STATE AS int),CAST(r.COUNT AS archive.Count),optional(r.LABEL),required(r.COUNTER),required(r.STATE) FROM (RECORDS) r`, 1)
	writeSourceHandlerFile(t, root, "archive/business.go", factoryAccessorSource)
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "records.db"))
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec("CREATE TABLE RECORDS(LABEL TEXT,COUNTER INTEGER,STATE INTEGER,COUNT INTEGER)")
	require.NoError(t, err)
	source.ColumnRefiner = column.New(column.Connections{"shape-discovery-only": db})
	var stable map[string]string
	for round := 0; round < 2; round++ {
		compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(context.Background(), source)
		require.NoError(t, err)
		_, err = (Generator{Operation: "post"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
		now := sourceHandlerSnapshot(t, root)
		require.Equal(t, factoryAccessorSource, now[filepath.Join(root, "archive/business.go")])
		if stable != nil {
			require.Equal(t, stable, now)
		}
		stable = now
	}
	writeSourceHandlerFile(t, root, "archive/accessors_test.go", factoryAccessorRuntime)
	command := exec.CommandContext(t.Context(), "go", "test", "-mod=readonly", "-race", "-count=1", "./archive")
	command.Dir = root
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s", output)
	for _, broken := range []string{
		strings.Replace(factoryAccessorSource, "r.Label=&v", "r.Label=missingValue", 1),
		factoryAccessorSource + "\nfunc incompatibleCaller(r *Record){r.SetLabel((*string)(nil))}\n",
		factoryAccessorSource + "\nfunc unresolvedType(v MissingType){}\n",
	} {
		writeSourceHandlerFile(t, root, "archive/business.go", broken)
		before := sourceHandlerSnapshot(t, root)
		compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(context.Background(), source)
		if err == nil {
			_, err = (Generator{Operation: "post"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
		}
		require.Error(t, err)
		require.Equal(t, before, sourceHandlerSnapshot(t, root), "failed staged typechecking changed destination")
	}
}

const factoryAccessorSource = `package archive
import("context";"github.com/viant/xdatly/handler")
type Count int
type State string
const Ready State="ready"
type OutputRes struct{Value string}
type Handler struct{}
func NewArchive()handler.Contract[ArchiveInput,ArchiveOutput]{return &Handler{}}
func(*Handler)Exec(context.Context,handler.Session,*ArchiveInput,*ArchiveOutput)error{return nil}
func(r *Record)SetLabel(v string){r.Label=&v;if r.Has==nil{r.Has=&RecordHas{}};r.Has.Label=true}
func(r Record)GetCounter()*int{return &r.Counter}
func(r *Record)SetCount(v int){r.Count=Count(v);if r.Has==nil{r.Has=&RecordHas{}};r.Has.Count=true}
func(r *Record)SetState(v State){r.State=0;if v==Ready{r.State=1};if r.Has==nil{r.Has=&RecordHas{}};r.Has.State=true}
func(r *Record)GetState()State{if r.State==1{return Ready};return ""}
`

const factoryAccessorRuntime = `package archive
import "testing"
type stateAccess interface{SetState(State);GetState()State}
func TestPreservedAccessors(t *testing.T){
 r:=&Record{}
 var domain stateAccess=r
 r.SetLabel("label")
 if r.Label==nil||*r.Label!="label"||!r.Has.Label{t.Fatal("pointer convenience or presence changed")}
 r.SetCounter(0)
 if *r.GetCounter()!=0||!r.Has.Counter{t.Fatal("scalar getter or untouched generated zero setter changed")}
 r.SetCount(7)
 if r.Count!=Count(7)||!r.Has.Count{t.Fatal("named conversion changed")}
 domain.SetState(Ready)
 if r.State!=1||domain.GetState()!=Ready||!r.Has.State{t.Fatal("domain ordinal mapping changed")}
 fresh:=&Record{}
 if fresh.GetLabel()!=nil||fresh.Has!=nil{t.Fatal("untouched generated getter changed presence")}
}
`
