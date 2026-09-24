package remote

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/provider/body"
	"github.com/viant/bindly/state"
)

// This proof uses the existing body provider and injector, not remote's
// hand-written JSON-pointer or reflection-based output assignment.
func TestNativeBinderMapsRemoteDocumentIntoContext(t *testing.T) {
	type authContext struct {
		ID              int              `json:"id"`
		AllowedEntities map[string][]int `json:"allowedEntities"`
	}
	type output struct{ Context *authContext }
	for _, test := range []struct {
		name, document string
		valid          bool
	}{
		{"valid", `{"user":{"id":7,"allowedEntities":{"project":[101,102]}}}`, true},
		{"wrong type", `{"user":{"id":"bad","allowedEntities":{"project":[101]}}}`, false},
		{"missing", `{}`, false},
		{"null", `{"user":null}`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			source, err := body.New([]byte(test.document), "application/json", nil, body.WithExactFieldNames())
			if err != nil {
				t.Fatal(err)
			}
			injector, err := bindly.NewInjector(bindly.WithProviders(source))
			if err != nil {
				t.Fatal(err)
			}
			required := true
			plan, err := injector.CompilePlan(reflect.TypeFor[output](), bindly.BindingSpec{Path: "Context", Location: state.Location{Kind: "body", In: "user"}, Required: &required})
			if err != nil {
				t.Fatal(err)
			}
			var result output
			err = injector.Bind(context.Background(), &result, bindly.WithPlan(plan))
			if !test.valid {
				if err == nil {
					t.Fatal("invalid remote document accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if result.Context == nil || result.Context.ID != 7 || !reflect.DeepEqual(result.Context.AllowedEntities["project"], []int{101, 102}) {
				t.Fatalf("context=%#v", result.Context)
			}
		})
	}
}
