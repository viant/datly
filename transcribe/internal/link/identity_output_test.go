package link

import (
	"github.com/viant/datly/spec"
	"testing"
)

func TestMergeCanonicalOutgoingIdentity(t *testing.T) {
	input := &spec.Parameter{Name: "Data", Source: spec.BindSource{Kind: "body", Name: "data"}}
	baseOut := &spec.Parameter{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "data"}, TypeExpr: "string"}
	authored := &spec.Parameter{Name: "Data", Source: spec.BindSource{Kind: "body", Name: "data"}, EmitOutput: true}
	inherited := &spec.Parameter{Name: "Inherited", Source: spec.BindSource{Kind: "query", Name: "key"}}
	got, err := (&Loader{}).mergeParams([]*spec.Parameter{input, baseOut, inherited}, []*spec.Parameter{input, authored})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[1].Source.Kind != "body" || !got[1].EmitOutput || got[1].TypeExpr != "string" || got[2] != inherited {
		t.Fatalf("merge %#v", got)
	}
	if baseOut.Source.Kind != "output" || baseOut.EmitOutput {
		t.Fatal("base mutated")
	}
	if _, err = (&Loader{}).mergeParams(nil, []*spec.Parameter{authored, baseOut}); err == nil {
		t.Fatal("equivalent outgoing duplicate accepted")
	}
	if _, err = (&Loader{}).mergeParams([]*spec.Parameter{authored, baseOut}, nil); err == nil {
		t.Fatal("equivalent package duplicate accepted")
	}
}
