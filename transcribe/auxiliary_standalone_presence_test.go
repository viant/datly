package transcribe

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// A sparse canonical auxiliary body must keep its own presence contract even
// when every independent read has a different public type.
func TestGeneratorStandaloneAuxiliaryBodyPresence(t *testing.T) {
	text := strings.Replace(auxiliaryOwnerDQL, "$Current<[]*Record>", "$Current<[]*CurrentRow>", 1)
	text = strings.Replace(text, "TypeName('Record')", "TypeName('CurrentRow')", 1)
	text = strings.Replace(text, "$Candidates<[]*Record>", "$Candidates<[]*CandidateRow>", 1)
	text = strings.Replace(text, "TypeName('Record')", "TypeName('CandidateRow')", 1)
	text = strings.Replace(text, "NULL AS note,NULL AS caller", "NULL AS note,NULL AS caller,NULL AS count,NULL AS enabled,NULL AS label", 1)
	text = strings.Replace(text, "tag(r.caller,'sqlx:\"-\" json:\"-\"')", "tag(r.caller,'sqlx:\"-\" json:\"-\"'),CAST(r.count AS int),tag(r.count,'sqlx:\"-\"'),CAST(r.enabled AS *bool),tag(r.enabled,'sqlx:\"-\"'),CAST(r.label AS *string),tag(r.label,'sqlx:\"-\"')", 1)
	root, source := auxiliaryOwnerFixture(t, text)
	var previous map[string]string
	for pass := 0; pass < 3; pass++ {
		generated, err := (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: source, Destination: root})
		if err != nil {
			t.Fatal(err)
		}
		for _, view := range generated.Result.Plan.Views {
			if view.Ownership == "shared-body" {
				t.Fatal("fixture acquired incidental body sharing")
			}
		}
		if pass == 0 {
			writeSourceFile(t, root, "generated/lifecycle.go", standaloneAuxiliaryHooks)
			continue
		}
		current := map[string]string{}
		for _, file := range generated.Result.Files {
			data, err := os.ReadFile(file.Path)
			if err != nil {
				t.Fatal(err)
			}
			current[file.Path] = string(data)
		}
		if previous != nil && !reflect.DeepEqual(previous, current) {
			t.Fatal("standalone auxiliary presence regeneration drift")
		}
		previous = current
	}
	entities, err := os.ReadFile(filepath.Join(root, "generated", "entities.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, signature := range []string{"func (entity *Record) SetCount(value int)", "func (entity *Record) SetEnabled(value *bool)", "func (entity *Record) SetLabel(value *string)"} {
		if strings.Count(string(entities), signature) != 1 {
			t.Fatalf("missing/duplicate accessor %s", signature)
		}
	}
	for _, unexpected := range []string{"CaptureInput", "OriginalInput", "OnInsert", "OnUpdate", "func (entity *CurrentRow)", "func (entity *CandidateRow)"} {
		if strings.Contains(string(entities), unexpected) {
			t.Fatal("auxiliary body acquired execution/read methods", unexpected)
		}
	}
	runtime := standaloneAuxiliaryRuntime()
	writeSourceFile(t, root, "generated/standalone_presence_runtime_test.go", runtime)
	cmd := exec.Command("go", "test", "-mod=mod", "-count=1", "./...")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("standalone native body presence runtime: %v\n%s", err, output)
	}
}

const standaloneAuxiliaryHooks = `package generated
import("context";"fmt";h "github.com/viant/xdatly/handler")
var ownerInitCalls,ownerValidateCalls int
type BatchLifecycle struct{}
func(*BatchLifecycle)Init(ctx context.Context,input *Input,state h.LifecycleContext[Input,h.NoParent,Output])error{
 ownerInitCalls++
 if state.Previous!=nil||state.Parent!=nil||state.Original!=nil{ return fmt.Errorf("auxiliary gained physical state") }
 if len(input.Current)!=1||input.Current[0].Id==nil||*input.Current[0].Id!=1||input.Current[0].TypeId==nil||*input.Current[0].TypeId!=7||len(input.Candidates)!=1||input.Candidates[0].Id==nil||*input.Candidates[0].Id!=2||input.Candidates[0].TypeId==nil||*input.Candidates[0].TypeId!=8{return fmt.Errorf("independent current projection changed")}
 state.Output.Data=input.Records;return nil
}
func(*BatchLifecycle)Validate(context.Context,*Input,h.LifecycleContext[Input,h.NoParent,Output])error{ownerValidateCalls++;return nil}
`

func standaloneAuxiliaryRuntime() string {
	text := strings.Replace(auxiliaryOwnerRuntime, "\"context\"", "\"context\";\"encoding/json\"", 1)
	a := strings.Index(text, " for _,name:=range []string{\"Records\",\"Current\",\"Candidates\"}")
	b := strings.Index(text, " for _,name:=range []string{\"Note\",\"Caller\"}")
	text = text[:a] + ` for _,test:=range []struct{name string;typ reflect.Type}{{"Records",reflect.TypeFor[[]*Record]()},{"Current",reflect.TypeFor[[]*CurrentRow]()},{"Candidates",reflect.TypeFor[[]*CandidateRow]()} }{field,ok:=reflect.TypeFor[Input]().FieldByName(test.name);if !ok||field.Type!=test.typ{t.Fatal("public independent type changed",test.name)}}
 if reflect.TypeFor[RecordHas]().NumField()!=7{t.Fatal("body marker field count changed")}
 for _,name:=range []string{"Id","Kind","Note","Caller","Count","Enabled","Label"}{if _,ok:=reflect.TypeFor[RecordHas]().FieldByName(name);!ok{t.Fatal("missing body marker",name)}}
 for _,typ:=range []reflect.Type{reflect.TypeFor[CurrentRow](),reflect.TypeFor[CandidateRow]()} {if typ.NumField()!=2{t.Fatal("independent read grew fields")};if _,ok:=typ.FieldByName("Has");ok{t.Fatal("independent read acquired marker")}}
` + text[b:]
	a = strings.Index(text, " request:=httptest.NewRequest")
	b = strings.Index(text[a:], " var count,total int;") + a
	text = text[:a] + ` for _,test:=range []struct{name,body string;present bool}{{"omitted",` + "`" + `{"data":[{"id":1}]}` + "`" + `,false},{"explicit zero false null",` + "`" + `{"data":[{"id":1,"count":0,"enabled":false,"label":null}]}` + "`" + `,true}}{
 t.Run(test.name,func(t *testing.T){request:=httptest.NewRequest("PATCH","/records",strings.NewReader(test.body));request.Header.Set("Content-Type","application/json");scope,err:=requestprovider.New(request);if err!=nil{t.Fatal(err)};defer scope.Close()
 value,err:=rt.ExecuteRoute(ctx,"PATCH","/records",scope);if err!=nil{t.Fatal(err)}
 row:=value.(*Output).Data[0];if row.Has==nil||!row.Has.Id||row.Has.Count!=test.present||row.Has.Enabled!=test.present||row.Has.Label!=test.present||row.Has.Kind||row.Has.Note||row.Has.Caller{t.Fatalf("sparse decoded markers: %+v",row.Has)}
 if row.Count!=0||row.Label!=nil{t.Fatal("zero/null decode changed")};if test.present&&(row.Enabled==nil||*row.Enabled){t.Fatal("explicit false lost")};if !test.present&&row.Enabled!=nil{t.Fatal("omission filled pointer")}
 encoded,err:=json.Marshal(row);if err!=nil{t.Fatal(err)};if strings.Contains(string(encoded),"Has")||strings.Contains(string(encoded),"has"){t.Fatal("markers exposed in JSON")}
 row.SetCount(0);row.SetEnabled(nil);row.SetLabel(nil);if !row.Has.Count||!row.Has.Enabled||!row.Has.Label{t.Fatal("authored setter presence lost")}
 })}
 if ownerInitCalls!=2||ownerValidateCalls!=2{t.Fatal("canonical lifecycle counts")}
` + text[b:]
	return text
}
