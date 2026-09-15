package transcribe

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestReadGroupsFollowCanonicalLinks(t *testing.T) {
	integer := spec.TypeRef{Name: "int"}
	parent := &plan.RecordPlan{Current: &plan.CurrentPlan{InputPath: plan.FieldPath{"Input", "Parents"}, Fields: []plan.CurrentField{{Entity: plan.FieldRef{Field: "Identity"}, Current: plan.FieldRef{Field: "RowKey"}}}}}
	child := &plan.RecordPlan{Current: &plan.CurrentPlan{InputPath: plan.FieldPath{"Input", "Children"}, Fields: []plan.CurrentField{{Entity: plan.FieldRef{Field: "Link"}, Current: plan.FieldRef{Field: "Belongs"}}, {Entity: plan.FieldRef{Field: "Tenant"}, Current: plan.FieldRef{Field: "Tenant"}}}}}
	parent.Relations = []*plan.RelationPlan{{Child: child, Links: []plan.KeyLink{{Parent: plan.KeyPart{Field: "Identity"}, Child: plan.KeyPart{Field: "Link"}}}}}
	reads := []plan.ReadCollection{{Name: "Parents", InputPath: parent.Current.InputPath, Fields: []plan.FieldRef{{Field: "RowKey", Type: integer}, {Field: "MisleadingId", Type: integer}}}, {Name: "Children", InputPath: child.Current.InputPath, Fields: []plan.FieldRef{{Field: "Belongs", Type: integer}, {Field: "Tenant", Type: integer}, {Field: "OtherId", Type: integer}}}}
	parent.Current.Keys = []plan.KeyPart{{Field: "RowKey", Type: integer}}
	reads[0].Keys = []plan.KeyPart{{Field: "MisleadingId", Type: integer}}
	if err := (&readGroupCompilation{reads: reads}).compile(parent); err != nil {
		t.Fatal(err)
	}
	if len(reads[0].Groups) != 1 || len(reads[1].Groups) != 1 || reads[0].Groups[0].Parts[0].Field != "RowKey" || reads[1].Groups[0].Parts[0].Field != "Belongs" {
		t.Fatalf("link projection or no-suffix policy lost: %+v", reads)
	}
	if len(reads[0].Keys) != 1 || reads[0].Keys[0].Field != "RowKey" {
		t.Fatal("canonical Current identity was not selected")
	}
	// Duplicate semantic edges do not duplicate the group or infer business use.
	parent.Relations = append(parent.Relations, parent.Relations[0])
	if err := (&readGroupCompilation{reads: reads}).compile(parent); err != nil {
		t.Fatal(err)
	}
	if len(reads[0].Groups) != 1 || len(reads[1].Groups) != 1 {
		t.Fatal("relationship group duplicated")
	}
	original := &plan.Plan{ReadCollections: reads}
	copy := original.Clone()
	copy.ReadCollections[0].Groups[0].Parts[0].Field = "changed"
	if !reflect.DeepEqual(original.ReadCollections, reads) || reads[0].Groups[0].Parts[0].Field != "RowKey" {
		t.Fatal("group parts aliased during semantic clone")
	}
}
