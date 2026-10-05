package writer_test

import (
	"context"
	"errors"
	"fmt"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/spec"
	sqlxio "github.com/viant/sqlx/io"
	h "github.com/viant/xdatly/handler"
	"reflect"
	"strings"
	"testing"
)

var dependentAllocation = errors.New("diagnostic: inherited child must not allocate")

// Diagnostic capabilities only: no SQL, no alternative allocator, no product
// persistence. They reveal the genuine native handler's allocation call order.
type diagnosticCapabilities struct {
	calls         []string
	queued        int
	parent, child int
}

func (*diagnosticCapabilities) Bind(context.Context, any) error { return nil }
func (c *diagnosticCapabilities) Lookup(_ context.Context, key h.ValueKey) (any, bool, error) {
	switch key {
	case h.FrameworkValidatorKey, h.TransactionStarterKey, h.SequencerKey, h.DMLKey:
		return c, true, nil
	}
	return nil, false, nil
}
func (*diagnosticCapabilities) Validate(context.Context, any, ...any) (*h.Validation, error) {
	return &h.Validation{}, nil
}
func (*diagnosticCapabilities) Start(context.Context) error  { return nil }
func (c *diagnosticCapabilities) Insert(string, any) error   { c.queued++; return nil }
func (c *diagnosticCapabilities) Update(string, any) error   { c.queued++; return nil }
func (c *diagnosticCapabilities) Delete(string, any) error   { c.queued++; return nil }
func (*diagnosticCapabilities) Execute(string, ...any) error { return nil }
func (c *diagnosticCapabilities) Allocate(_ context.Context, table string, dest any, selector string) error {
	c.calls = append(c.calls, table+":"+selector)
	parent := reflect.ValueOf(dest).Index(0).Elem()
	if table == "parents" {
		parent.FieldByName("Id").SetInt(123)
		return nil
	}
	c.parent = int(parent.FieldByName("Id").Int())
	c.child = int(parent.FieldByName("Child").Elem().FieldByName("ParentId").Int())
	return dependentAllocation
}
func TestNativeInheritedKeyUsesOnlyParentAllocation(t *testing.T) {
	inType, outType := contracts("parent_id,primaryKey=true,refTable=parents,refColumn=id")
	handler, err := writer.New(component(""), inType, outType, "post")
	if err != nil {
		t.Fatal(err)
	}
	input := reflect.New(inType)
	rows := input.Elem().FieldByName("Rows")
	parent := reflect.New(rows.Type().Elem().Elem())
	parent.Elem().FieldByName("Child").Set(reflect.New(parent.Elem().FieldByName("Child").Type().Elem()))
	rows.Set(reflect.Append(rows, parent))
	snapshot, err := handler.CaptureInput(t.Context(), input.Interface())
	if err != nil {
		t.Fatal(err)
	}
	caps := &diagnosticCapabilities{}
	result, err := handler.Execute(t.Context(), rhandler.Invocation{Input: input.Interface(), Snapshot: snapshot, Binder: caps})
	if err != nil || result == nil || fmt.Sprint(caps.calls) != "[parents:Id]" || caps.queued != 2 || parent.Elem().FieldByName("Child").Elem().FieldByName("ParentId").Int() != 123 {
		t.Fatalf("order=%v queued=%d input=%v result=%T err=%v", caps.calls, caps.queued, input.Interface(), result, err)
	}
}

// Generic diagnostic contracts, not Platform authoring or a proposed fix.
// SQL identity and FK ownership match the original parent/shared-key child.
func contracts(childSQLX string) (reflect.Type, reflect.Type) {
	intType := reflect.TypeFor[int]()
	child := reflect.StructOf([]reflect.StructField{
		{Name: "ParentId", Type: intType, Tag: reflect.StructTag(`sqlx:"` + childSQLX + `"`)},
		{Name: "Deleted", Type: reflect.TypeFor[bool](), Tag: `sqlx:"deleted" writer:"delete"`},
	})
	parent := reflect.StructOf([]reflect.StructField{
		{Name: "Id", Type: intType, Tag: `sqlx:"id,primaryKey=true,autoincrement=true"`},
		{Name: "Child", Type: reflect.PointerTo(child), Tag: `view:"Child,table=children" on:"Id=ParentId"`},
	})
	input := reflect.StructOf([]reflect.StructField{
		{Name: "Rows", Type: reflect.SliceOf(reflect.PointerTo(parent)), Tag: `parameter:"Rows,kind=body,in=data" view:"Rows,table=parents"`},
		{Name: "CurrentRows", Type: reflect.SliceOf(reflect.PointerTo(parent)), Tag: `parameter:"CurrentRows,kind=view" view:"CurrentRows,table=parents"`},
		{Name: "CurrentChild", Type: reflect.SliceOf(reflect.PointerTo(child)), Tag: `parameter:"CurrentChild,kind=view" view:"CurrentChild,table=children"`},
	})
	output := reflect.StructOf([]reflect.StructField{{Name: "Data", Type: reflect.SliceOf(reflect.PointerTo(parent)), Tag: `parameter:"Data,kind=output,in=body"`}})
	return input, output
}
func component(policy string) *spec.Component {
	return &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "allocator.audit", Name: "Rows"}, Settings: &spec.Settings{Mutation: "post"}, RootView: &spec.View{Name: "Rows", Source: &spec.ViewSource{Table: "parents"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true, AutoIncrement: true}}, Relations: []*spec.Relation{{Name: "Child", Holder: "Child", View: &spec.View{Name: "Child", WriterIdentityPolicy: policy, Source: &spec.ViewSource{Table: "children"}, Columns: []*spec.Column{{Name: "parent_id", Source: "parent_id", PrimaryKey: true, AutoIncrement: false}, {Name: "deleted", Source: "deleted", DeleteMarker: true}}}}}}}
}
func TestNativeInheritedSharedKeyPreservesIdentityMetadata(t *testing.T) {
	for _, operation := range []string{"post", "patch", "put"} {
		input, output := contracts("parent_id,primaryKey=true,refTable=parents,refColumn=id")
		c := component("")
		c.Settings.Mutation = operation
		m, err := writer.Compile(c, input, output, operation)
		if err != nil {
			t.Fatal(err)
		}
		root := m.Root
		child := root.Relations[0].Child
		if root.Sequence == nil || !root.Sequence.AutoIncrement || root.Sequence.Name != "Id" {
			t.Fatal("genuine parent allocator metadata lost")
		}
		if child.Sequence != nil || child.Selector != "" {
			t.Fatal("inherited identity still allocated", child.Sequence, child.Selector)
		}
		if len(child.Keys) != 1 || child.Keys[0].RefTable != "parents" || child.Keys[0].RefColumn != "id" || child.CurrentField < 0 || child.DeleteMarker == nil {
			t.Fatal("source identity/current/delete metadata lost")
		}

	}
}
func TestExistingPoliciesCannotSuppressPostChildSequence(t *testing.T) {
	input, output := contracts("parent_id,primaryKey=true,refTable=parents,refColumn=id")
	_, err := writer.Compile(component("assigned-update"), input, output, "post")
	if err == nil || !strings.Contains(err.Error(), "requires a PATCH writable role") {
		t.Fatalf("post assigned-update restriction: %v", err)
	}
	_, err = writer.Compile(component("none"), input, output, "post")
	if err == nil || !strings.Contains(err.Error(), "must be assigned-update") {
		t.Fatalf("identity none unsupported: %v", err)
	}
	t.Log("assigned-update requires PATCH and changes insert behavior; arbitrary none policy rejected")
	input, output = contracts("parent_id,primaryKey=false,refTable=parents,refColumn=id")
	_, err = writer.Compile(component(""), input, output, "post")
	if err == nil || !strings.Contains(err.Error(), "has no primary key") {
		t.Fatalf("removing actual shared PK must reject: %v", err)
	}
	t.Log("primaryKey=false removes required child identity, not a supported suppression")
}
func TestSQLXSequenceFalseAndAutoFalseAreNotDisablingPolicies(t *testing.T) {
	tag := sqlxio.ParseTag(reflect.StructTag(`sqlx:"parent_id,primaryKey=true,sequence=false"`))
	if tag.Sequence != "false" {
		t.Fatalf("literal sequence identity: %q", tag.Sequence)
	}
	tag = sqlxio.ParseTag(reflect.StructTag(`sqlx:"parent_id,primaryKey=true,autoincrement=false"`))
	if !tag.Autoincrement {
		t.Fatal("existing SQLX recognizes autoincrement token regardless literal false")
	}
	input, output := contracts("parent_id,primaryKey=true,refTable=parents,refColumn=id,sequence=false")
	m, err := writer.Compile(component(""), input, output, "post")
	if err != nil {
		t.Fatal(err)
	}
	if m.Root.Relations[0].Child.Sequence == nil {
		t.Fatal("sequence=false unexpectedly suppresses native metadata")
	}
	t.Log("sequence=false is a literal SQLX sequence name; native writer still synthesizes the numeric PK Sequence")
}

func TestInheritedKeyExcludesIndependentAndAmbiguousIdentities(t *testing.T) {
	for _, tc := range []struct{ name, tag, table, on string }{
		{name: "explicit auto", tag: "parent_id,primaryKey=true,refTable=parents,refColumn=id,autoincrement=true"},
		{name: "named sequence", tag: "parent_id,primaryKey=true,refTable=parents,refColumn=id,sequence=own"},
		{name: "generator", tag: "parent_id,primaryKey=true,refTable=parents,refColumn=id,generator=own"},
		{name: "missing FK", tag: "parent_id,primaryKey=true"},
		{name: "wrong table", tag: "parent_id,primaryKey=true,refTable=others,refColumn=id"},
		{name: "wrong column", tag: "parent_id,primaryKey=true,refTable=parents,refColumn=other"},
		{name: "different case", tag: "parent_id,primaryKey=true,refTable=PARENTS,refColumn=id"},
		{name: "explicit schema", tag: "parent_id,primaryKey=true,refDb=s,refTable=parents,refColumn=id"},
		{name: "qualified target", tag: "parent_id,primaryKey=true,refTable=s.parents,refColumn=id", table: "s.parents"},
		{name: "duplicate links", tag: "parent_id,primaryKey=true,refTable=parents,refColumn=id", on: "Id=ParentId,Id=ParentId"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in, out := contracts(tc.tag)
			c := component("")
			if tc.table != "" {
				c.RootView.Source.Table = tc.table
			}
			if tc.on != "" {
				fields := make([]reflect.StructField, in.NumField())
				for i := range fields {
					fields[i] = in.Field(i)
				}
				parent := fields[0].Type.Elem().Elem()
				pfields := []reflect.StructField{parent.Field(0), parent.Field(1)}
				pfields[1].Tag = reflect.StructTag(`view:"Child,table=children" on:"` + tc.on + `"`)
				p := reflect.StructOf(pfields)
				for i := 0; i < 2; i++ {
					fields[i].Type = reflect.SliceOf(reflect.PointerTo(p))
				}
				in = reflect.StructOf(fields)
				out = reflect.StructOf([]reflect.StructField{{Name: "Data", Type: fields[0].Type, Tag: `parameter:"Data,kind=output,in=body"`}})
			}
			m, err := writer.Compile(c, in, out, "post")
			if err != nil {
				t.Fatal(err)
			}
			child := m.Root.Relations[0].Child
			if child.Sequence == nil || child.Selector == "" {
				t.Fatal("excluded identity lost independent allocation")
			}
		})
	}
}
