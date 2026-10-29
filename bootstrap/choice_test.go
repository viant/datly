package bootstrap

import (
	"encoding/json"
	"github.com/viant/datly/spec"
	"reflect"
	"strings"
	"testing"
)

type linkedOrderedChoices struct {
	Number      int    `parameter:"Number,kind=query,in=number,value=3" choice:"3" choice:"1" choice:"2"`
	Text        string `parameter:"Text,kind=query,in=text" choice:"z" choice:"a" choice:"m"`
	NumericText string `parameter:"NumericText,kind=query,in=numericText" choice:"3" choice:"1" choice:"2"`
	Flag        *bool  `parameter:"Flag,kind=query,in=flag,required=false" choice:"false" choice:"true"`
	Big         int64  `parameter:"Big,kind=query,in=big" choice:"9007199254740993" choice:"9007199254740994"`
}

type namedNumberChoice int64

func (namedNumberChoice) MarshalText() ([]byte, error) { return []byte("wrong-wire-value"), nil }
func TestLinkedNamedChoiceHasPlainWireValues(t *testing.T) {
	type input struct {
		Mode *namedNumberChoice `parameter:"Mode,kind=query,in=mode,required=false" choice:"3" choice:"1" choice:"2"`
	}
	c, e := (ContractResolver{InputType: linkedContractType(reflect.TypeFor[input]())}).Resolve()
	if e != nil {
		t.Fatal(e)
	}
	if len(c.Parameters) != 1 || c.Parameters[0].WireSchema == nil {
		t.Fatal("metadata missing")
	}
	b, _ := json.Marshal(c.Parameters[0].WireSchema.Enum)
	if string(b) != "[3,1,2]" {
		t.Fatal(string(b))
	}
}

func TestLinkedOrderedChoiceContract(t *testing.T) {
	c, e := (ContractResolver{InputType: linkedContractType(reflect.TypeFor[linkedOrderedChoices]())}).Resolve()
	if e != nil {
		t.Fatal(e)
	}
	wants := map[string]string{"Number": "[3,1,2]", "Text": `["z","a","m"]`, "NumericText": `["3","1","2"]`, "Flag": "[false,true]", "Big": "[9007199254740993,9007199254740994]"}
	for _, p := range c.Parameters {
		w, ok := wants[p.Name]
		if !ok {
			continue
		}
		if p.WireSchema == nil {
			t.Fatal(p.Name)
		}
		b, _ := json.Marshal(p.WireSchema.Enum)
		if string(b) != w {
			t.Fatalf("%s=%s want=%s", p.Name, b, w)
		}
		delete(wants, p.Name)
	}
	if len(wants) > 0 {
		t.Fatal(wants)
	}
}
func TestLinkedChoiceConflictsFailClosed(t *testing.T) {
	for _, schema := range []*spec.WireSchema{{Type: "integer", Enum: []any{3, 2, 1}}, {Type: "string", Enum: []any{"3", "1", "2"}}} {
		c := &spec.Component{Parameters: []*spec.Parameter{{Name: "Number", Source: spec.BindSource{Kind: "query", Name: "number"}, WireSchema: schema}}}
		_, e := (ContractResolver{Component: c, InputType: linkedContractType(reflect.TypeFor[linkedOrderedChoices]())}).Resolve()
		if e == nil || !strings.Contains(e.Error(), "choice") {
			t.Fatalf("conflict accepted: %v", e)
		}
	}
}
