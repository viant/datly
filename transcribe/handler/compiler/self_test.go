package compiler

import (
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"strings"
	"testing"
)

func TestCanonicalSelfLinksAndLocalSequence(t *testing.T) {
	for _, tc := range []struct{ name, parent, typ, want string }{{"nullable parent", "PARENT_ID", "int64", ""}, {"missing parent", "MISSING", "int64", "no link column"}, {"incompatible parent", "PARENT_ID", "string", "incompatible"}} {
		t.Run(tc.name, func(t *testing.T) {
			component := testComponent()
			component.RootView.Columns = append(component.RootView.Columns, &spec.Column{Name: "PARENT_ID", Source: "PARENT_ID", Type: spec.TypeRef{Name: tc.typ, Pointer: true}})
			component.RootView.SelfReference = &spec.SelfReference{Holder: "Children", Child: "ID", Parent: tc.parent}
			compiled, err := (&Compiler{}).Compile(Request{Component: component, Operation: plan.OperationPost})
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("err%v want%s", err, tc.want)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			root := compiled.Root
			if root.Sequence == nil || root.Sequence.Field.Field != "Id" || len(root.SelfRelations) != 1 {
				t.Fatalf("metadata %+v", root)
			}
			link := root.SelfRelations[0].Links[0]
			if link.Parent.Field != "Id" || link.Child.Field != "ParentId" || link.Conversion != plan.LinkAddress {
				t.Fatalf("link %+v", link)
			}
			cloned := compiled.Clone()
			cloned.Root.SelfRelations[0].FieldPath[0] = "Other"
			cloned.Root.SelfRelations[0].Links[0].Child.Field = "Other"
			if root.SelfRelations[0].FieldPath[0] != "Children" || root.SelfRelations[0].Links[0].Child.Field != "ParentId" {
				t.Fatal("clone aliases self metadata")
			}
		})
	}
}
