package output

import (
	"context"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestHTTPCompressionPolicyDoesNotChangeEncodedOutput(t *testing.T) {
	type result struct {
		Value string `json:"value"`
	}
	policy := &spec.ResponseCompression{Encoding: "gzip", MinSizeBytes: 2}
	p, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[result](), Component: &spec.Component{Settings: &spec.Settings{ResponseCompression: policy}}})
	if err != nil {
		t.Fatal(err)
	}
	policy.MinSizeBytes = 2000
	p.ResponseCompression().MinSizeBytes = 1
	if p.ResponseCompression().MinSizeBytes != 2 {
		t.Fatal("registered authority is mutable")
	}
	out, err := p.Encode(context.Background(), "json", &result{Value: "literal"})
	if err != nil || string(out.Data) != `{"value":"literal"}` {
		t.Fatalf("non-HTTP encoding was compressed: %s %v", out.Data, err)
	}
	if _, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[result](), Component: &spec.Component{Settings: &spec.Settings{ResponseCompression: &spec.ResponseCompression{Encoding: "invalid"}}}}); err == nil {
		t.Fatal("invalid registered policy accepted")
	}
}
