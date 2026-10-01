package generate

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func clientProjectionComponent(enabled bool) *spec.Component {
	name := ""
	if enabled {
		name = "RecordsRequest"
	}
	required, optional := true, false
	defaultValue := "true"
	result := &spec.Component{Name: "Records", Settings: &spec.Settings{InputType: "ServerInput", CaseFormat: "lc", Generation: &spec.GenerationSettings{ClientInputType: name}}, Parameters: []*spec.Parameter{
		{Name: "Id", TypeExpr: "string", Source: spec.BindSource{Kind: "path", Name: "id"}, Required: &required, Tag: `json:"-"`},
		{Name: "Enabled", TypeExpr: "bool", Source: spec.BindSource{Kind: "query", Name: "enabled"}, Required: &optional, Value: &defaultValue, Codec: &spec.Codec{Body: "FlagCodec", OutputType: "bool"}, Predicates: []*spec.Predicate{{Group: 2, Name: "equal", Args: []string{"r", "enabled"}}}},
		{Name: "Body", TypeExpr: "map[string]string", Source: spec.BindSource{Kind: "body"}},
		{Name: "InternalQuery", TypeExpr: "bool", Source: spec.BindSource{Kind: "query", Name: "internal"}, Tag: `internal:"true"`},
		{Name: "PrivateBody", TypeExpr: "string", Source: spec.BindSource{Kind: "body"}, Tag: `private:"true"`},
	}}
	for _, kind := range []string{"visibility", "readerAccess", "ctx", "env", "const", "param", "data_view", "output", "component", "unknown"} {
		result.Parameters = append(result.Parameters, &spec.Parameter{Name: exportedName(kind) + "Secret", TypeExpr: "string", Source: spec.BindSource{Kind: kind, Name: "secret"}})
	}
	return result
}
func TestClientInputProjectionProtectedFieldsAndCanonicalInput(t *testing.T) {
	before := testPlan(t, clientProjectionComponent(false))
	after := testPlan(t, clientProjectionComponent(true))
	if before.ClientInput != nil {
		t.Fatal("default enabled client projection")
	}
	if !reflect.DeepEqual(before.Input, after.Input) {
		t.Fatal("client projection changed runtime input")
	}
	if after.ClientInput.Type != "RecordsRequest" || len(after.ClientInput.Fields) != 3 {
		t.Fatalf("public fields %+v", after.ClientInput)
	}
	field, _ := after.ClientInput.Field("Enabled")
	for _, required := range []string{"kind=query", "in=enabled", "value=true", "required=false", "predicate:", "codec:", `json:"enabled"`} {
		if !strings.Contains(field.Tag, required) {
			t.Fatalf("lost metadata %s in %s", required, field.Tag)
		}
	}
	if field.Type != "bool" {
		t.Fatalf("field type %s", field.Type)
	}
	content := inputStructFile("records", "", "RecordsRequest", after.ClientInput.Fields, after.Imports)
	for _, param := range clientProjectionComponent(true).Parameters[3:] {
		if strings.Contains(content, param.Name) {
			t.Fatalf("protected name %s exposed", param.Name)
		}
	}
	dir := t.TempDir()
	mustWrite := func(name, data string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	mustWrite("client_input.go", content)
	mustWrite("client_input_setters.go", inputSetterFile("records", "RecordsRequest", after.ClientInput.Fields, after.Imports))
	mustWrite("client_test.go", `package records
import("encoding/json";"testing")
func TestPresence(t *testing.T){r:=&RecordsRequest{};if r.Has!=nil{t.Fatal("absence lost")};r.SetEnabled(false);r.SetId("");r.SetBody(nil);if r.Has==nil||!r.Has.Enabled||!r.Has.Id||!r.Has.Body||r.Enabled||r.Id!=""||r.Body!=nil{t.Fatal("explicit zero/null presence lost")};raw,e:=json.Marshal(r);if e!=nil{t.Fatal(e)};var fields map[string]any;if e=json.Unmarshal(raw,&fields);e!=nil{t.Fatal(e)};if value,ok:=fields["enabled"];!ok||value!=false{t.Fatalf("zero omitted: %s",raw)};if _,ok:=fields["Has"];ok{t.Fatal("marker leaked")};if _,ok:=fields["has"];ok{t.Fatal("marker leaked")}}
`)
	command := exec.Command("go", "test", "client_input.go", "client_input_setters.go", "client_test.go")
	command.Dir = dir
	command.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("compiled public input: %v\n%s", err, output)
	}
}
func TestClientInputProjectionRejectsTypeCollision(t *testing.T) {
	component := clientProjectionComponent(true)
	component.Settings.Generation.ClientInputType = "ServerInput"
	if _, err := New(Input{Component: component}).Plan(); err == nil || !strings.Contains(err.Error(), "shared") {
		t.Fatalf("collision error %v", err)
	}
}

func TestClientInputProjectionScaffoldIsOptIn(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		plan := testPlan(t, clientProjectionComponent(enabled))
		files, _, _, err := scaffoldArtifacts(t.TempDir(), plan)
		if err != nil {
			t.Fatal(err)
		}
		emitted := false
		for _, file := range files {
			if filepath.Base(file.Path) == "client_input.go" {
				emitted = true
				if !strings.Contains(file.Content, "type RecordsRequest struct") || strings.Contains(file.Content, "VisibilitySecret") {
					t.Fatalf("client artifact %s", file.Content)
				}
			}
			if filepath.Base(file.Path) == plan.RouterDest && strings.Contains(file.Content, "RecordsRequest") {
				t.Fatal("public projection replaced runtime holder")
			}
		}
		if emitted != enabled {
			t.Fatalf("enabled=%v emitted=%v", enabled, emitted)
		}
	}
}
