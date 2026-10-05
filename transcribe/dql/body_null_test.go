package dql

import (
	"encoding/json"
	"github.com/viant/datly/spec"
	"testing"
)

func TestBodyNullPolicyDeclarationRoundTrip(t *testing.T) {
	source := "#setting($_ = $route('/records','PATCH'))\n#define($_ = $View<*View>(body/).Required().WithBodyNullPolicy('empty-record'))\nSELECT id FROM records"
	component, err := parseComponentSource("example.com/records", "Records", source)
	if err != nil {
		t.Fatal(err)
	}
	params := spec.EffectiveParameters(component.Parameters)
	if len(params) != 1 || params[0].BodyNullPolicy != "empty-record" || params[0].Required == nil || !*params[0].Required {
		t.Fatalf("params=%+v", params)
	}
	encoded, err := json.Marshal(component.Clone())
	if err != nil {
		t.Fatal(err)
	}
	var reloaded spec.Component
	if err = json.Unmarshal(encoded, &reloaded); err != nil {
		t.Fatal(err)
	}
	if reloaded.Parameters[0].BodyNullPolicy != "empty-record" {
		t.Fatal(string(encoded))
	}
	exported := (Serializer{}).Export(&reloaded, source)
	reparsed, err := parseComponentSource("example.com/records", "Records", exported.Source)
	if err != nil {
		t.Fatal(err)
	}
	if reparsed.Parameters[0].BodyNullPolicy != "empty-record" {
		t.Fatal("DQL reload lost policy")
	}
}
func TestBodyNullPolicyRejectsInvalidDeclarations(t *testing.T) {
	for _, declaration := range []string{
		"$View<*View>(body/).WithBodyNullPolicy('other')", "$View<*View>(body/name).WithBodyNullPolicy('empty-record')", "$View<*View>(query/view).WithBodyNullPolicy('empty-record')", "$View<*View>(body/).WithBodyNullPolicy('empty-record').WithCodec('JSON')", "$View<*View>(body/).WithBodyNullPolicy('empty-record').WithBodyNullPolicy('empty-record')",
	} {
		if _, err := parseComponentSource("example.com/records", "Records", "#define($_ = "+declaration+")\nSELECT id FROM records"); err == nil {
			t.Fatalf("accepted %s", declaration)
		}
	}
}
