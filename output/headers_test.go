package output

import (
	"context"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	"reflect"
	"strings"
	"testing"
)

func TestStaticHeaderValidationAndIsolation(t *testing.T) {
	for _, headers := range []map[string]string{
		{"Bad Name": "x"}, {"X-Test": "x\r\ny"}, {"X-Test": "x\x00y"}, {"X-Test": "a", "x-test": "b"},
		{"Content-Type": "x"}, {"Content-Length": "7"}, {"Transfer-Encoding": "chunked"}, {"Content-Encoding": "gzip"}, {"Content-Disposition": "x"}, {"Connection": "x"}, {"Access-Control-Allow-Origin": "*"}, {"Vary": "*"},
	} {
		_, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[struct{ URL string }](), Component: &spec.Component{Settings: &spec.Settings{Output: &spec.OutputSettings{Headers: headers}}}})
		if err == nil {
			t.Fatalf("accepted unsafe metadata %v", headers)
		}
	}
	settings := tag.Settings{Output: &spec.OutputSettings{Headers: map[string]string{"cache-control": "no-store"}}}
	text, err := settings.StructTag()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := tag.ParseSettings(reflect.StructTag(text))
	if err != nil {
		t.Fatal(err)
	}
	c := &spec.Component{Settings: &spec.Settings{Output: parsed.Output}}
	clone := c.Clone()
	clone.Settings.Output.Headers["cache-control"] = "clone"
	if c.Settings.Output.Headers["cache-control"] != "no-store" {
		t.Fatal("clone alias")
	}
	p, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[struct {
		URL string `json:"url"`
	}](), Component: c})
	if err != nil {
		t.Fatal(err)
	}
	c.Settings.Output.Headers["cache-control"] = "changed"
	got := p.Headers()
	got["Cache-Control"] = "mutated"
	if p.Headers()["Cache-Control"] != "no-store" {
		t.Fatal("plan metadata alias")
	}
	encoded, err := p.Encode(context.Background(), "json", struct {
		URL string `json:"url"`
	}{"url"})
	if err != nil || strings.Contains(string(encoded.Data), "headers") || string(encoded.Data) != `{"url":"url"}` {
		t.Fatalf("%s %v", encoded.Data, err)
	}
}
