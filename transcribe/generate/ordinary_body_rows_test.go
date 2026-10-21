package generate

import (
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func ordinaryBodyPlanner() (*viewPlanner, *spec.View, *spec.View) {
	source := func() *spec.ViewSource {
		return &spec.ViewSource{Table: "records", Bindings: &spec.ViewBindings{Connector: "main"}}
	}
	owner := &spec.View{Name: "Body", TypeName: "Record", Source: source(), Columns: []*spec.Column{{Name: "Id", Source: "id", DatabaseType: "INTEGER", Type: spec.TypeRef{Name: "int"}, ExplicitType: true}, {Name: "Purge", Source: "purge", Type: spec.TypeRef{Name: "bool"}, ExplicitType: true, DeleteMarker: true, Tag: `sqlx:"-" diff:"-" internal:"true" json:"-"`}}}
	current := &spec.View{Name: "PreviousByKey", TypeName: "Record", Source: source(), Columns: []*spec.Column{{Name: "Id", Source: "id", DatabaseType: "INTEGER", Type: spec.TypeRef{Name: "int"}, ExplicitType: true}}}
	p := &viewPlanner{plan: &Plan{Package: "example.com/rows", Connector: "main"}, names: map[*spec.View]string{owner: "Record"}, dests: map[*spec.View]string{owner: "record.go"}, readTypes: map[*spec.View][]string{current: {"Record"}}, nativeMutation: true, requireConcrete: true}
	return p, owner, current
}

func TestOrdinarySharedBodyGuards(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		change     func(*viewPlanner, *spec.View, *spec.View)
	}{
		{name: "native marker and complete business read"},
		{name: "different table", want: "table compatibility", change: func(_ *viewPlanner, _ *spec.View, c *spec.View) { c.Source.Table = "other_records" }},
		{name: "missing table", want: "table compatibility", change: func(_ *viewPlanner, _ *spec.View, c *spec.View) { c.Source.Table = "" }},
		{name: "different connector", want: "connector compatibility", change: func(_ *viewPlanner, _ *spec.View, c *spec.View) { c.Source.Bindings.Connector = "other" }},
		{name: "missing connector", want: "connector compatibility", change: func(p *viewPlanner, b, c *spec.View) {
			p.plan.Connector = ""
			b.Source.Bindings.Connector = ""
			c.Source.Bindings.Connector = ""
		}},
		{name: "extra body business field", want: "coverage", change: func(_ *viewPlanner, b, _ *spec.View) {
			b.Columns = append(b.Columns, &spec.Column{Name: "Extra", Source: "extra", Type: spec.TypeRef{Name: "int"}})
		}},
		{name: "extra read business field", want: "coverage", change: func(_ *viewPlanner, _ *spec.View, c *spec.View) {
			c.Columns = append(c.Columns, &spec.Column{Name: "Extra", Source: "extra", Type: spec.TypeRef{Name: "int"}})
		}},
		{name: "unclassified internal transient", want: "unclassified transient", change: func(_ *viewPlanner, b, _ *spec.View) { b.Columns[1].DeleteMarker = false }},
		{name: "unknown transient tag", want: "tags conflict", change: func(_ *viewPlanner, b, _ *spec.View) { b.Columns[0].Tag = `diff:"-"` }},
		{name: "explicit incompatible cast", want: "canonical scalar", change: func(_ *viewPlanner, _ *spec.View, c *spec.View) { c.Columns[0].Type.Name = "string" }},
		{name: "inferred pointer mismatch cannot borrow physical exception", want: "canonical scalar", change: func(_ *viewPlanner, _ *spec.View, c *spec.View) {
			c.Columns[0].Type.Pointer = true
			c.Columns[0].ExplicitType = false
		}},
		{name: "inferred scalar mismatch cannot borrow physical exception", want: "canonical scalar", change: func(_ *viewPlanner, _ *spec.View, c *spec.View) {
			c.Columns[0].Type.Name = "int64"
			c.Columns[0].ExplicitType = false
		}},
		{name: "same pointer type", change: func(_ *viewPlanner, b, c *spec.View) {
			b.Columns[0].Type.Pointer = true
			c.Columns[0].Type.Pointer = true
		}},
		{name: "unrelated public rename", want: "tags conflict", change: func(_ *viewPlanner, b, _ *spec.View) { b.Columns[0].Tag = `json:"different"` }},
		{name: "native internal visibility", change: func(_ *viewPlanner, b, _ *spec.View) { b.Columns[0].Tag = `internal:"true" json:"-"` }},
		{name: "native internal with explicit read conflict", want: "tags conflict", change: func(_ *viewPlanner, b, c *spec.View) {
			b.Columns[0].Tag = `internal:"true" json:"-"`
			c.Columns[0].Tag = `json:"visible"`
		}},
		{name: "json hidden alone", want: "tags conflict", change: func(_ *viewPlanner, b, _ *spec.View) { b.Columns[0].Tag = `json:"-"` }},
		{name: "body validation policy retained", change: func(_ *viewPlanner, b, _ *spec.View) { b.Columns[0].Tag = `validate:"required"` }},
		{name: "explicit read validation conflict", want: "tags conflict", change: func(_ *viewPlanner, b, c *spec.View) {
			b.Columns[0].Tag = `validate:"required"`
			c.Columns[0].Tag = `validate:"gt(1)"`
		}},
		{name: "unknown semantic tag", want: "tags conflict", change: func(_ *viewPlanner, b, _ *spec.View) { b.Columns[0].Tag = `unknown:"semantic"` }},
		{name: "missing physical mapping", want: "column mapping", change: func(_ *viewPlanner, _ *spec.View, c *spec.View) { c.Columns[0].Source = "" }},
		{name: "ambiguous physical mapping", want: "ambiguous column", change: func(_ *viewPlanner, b, _ *spec.View) {
			b.Columns = append(b.Columns, &spec.Column{Name: "OtherId", Source: "id", Type: spec.TypeRef{Name: "int"}})
		}},
		{name: "self reference", want: "independent leaf", change: func(_ *viewPlanner, _ *spec.View, c *spec.View) { c.SelfReference = &spec.SelfReference{} }},
		{name: "body self reference", want: "body must remain", change: func(_ *viewPlanner, b, _ *spec.View) { b.SelfReference = &spec.SelfReference{} }},
		{name: "body relation", want: "body must remain", change: func(_ *viewPlanner, b, _ *spec.View) {
			b.Relations = []*spec.Relation{{Holder: "Child", View: &spec.View{Name: "Child"}}}
		}},
		{name: "read relation", want: "independent leaf", change: func(_ *viewPlanner, _ *spec.View, c *spec.View) {
			c.Relations = []*spec.Relation{{Holder: "Child", View: &spec.View{Name: "Child"}}}
		}},
		{name: "final without normalized native target", want: "normalized native", change: func(p *viewPlanner, _ *spec.View, _ *spec.View) { p.nativeMutation = false }},
		{name: "staged unknown read", change: func(p *viewPlanner, _ *spec.View, c *spec.View) {
			p.nativeMutation = false
			p.requireConcrete = false
			c.Columns = nil
		}},
		{name: "unproven provisional metadata", want: "native delete", change: func(p *viewPlanner, b, _ *spec.View) {
			p.nativeMutation = false
			p.requireConcrete = false
			b.Columns[1].DeleteMarker = false
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, b, c := ordinaryBodyPlanner()
			if tc.change != nil {
				tc.change(p, b, c)
			}
			err := p.shareOrdinaryBodyRow(c, b, "record.go")
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v want guard %q", err, tc.want)
			}
		})
	}
}

func TestOrdinarySharedBodyNullableUpdatedPresentation(t *testing.T) {
	for _, tc := range []struct {
		name, readTag string
		wantError     bool
	}{
		{name: "source value time inferred omission"},
		{name: "explicit read omission conflicts", readTag: `json:"updated,omitempty"`, wantError: true},
		{name: "explicit read rename conflicts", readTag: `json:"changed"`, wantError: true},
		{name: "same authored policy", readTag: `json:"updated"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, b, c := ordinaryBodyPlanner()
			p.plan.Settings.Mutation = "patch"
			p.plan.Settings.CaseFormat = "lc"
			for _, v := range []*spec.View{b, c} {
				v.Columns[0].Name = "UPDATED"
				v.Columns[0].Source = "UPDATED"
				v.Columns[0].DatabaseType = "DATETIME"
				v.Columns[0].Type = spec.TypeRef{Package: "time", Name: "Time"}
				v.Columns[0].Nullable = true
			}
			b.Columns[0].Tag = `json:"updated"`
			c.Columns[0].Tag = tc.readTag
			before := b.Columns[0].Tag
			fields, err := p.fields(c)
			if err != nil {
				t.Fatal(err)
			}
			if tc.readTag == "" && !strings.Contains(fields[0].Tag, `json:"updated,omitempty"`) {
				t.Fatalf("native inferred source policy not exercised: %s", fields[0].Tag)
			}
			err = p.shareOrdinaryBodyRow(c, b, "record.go")
			if tc.wantError {
				if err == nil || !strings.Contains(err.Error(), "tags conflict") {
					t.Fatalf("explicit read policy lost: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if b.Columns[0].Tag != before {
				t.Fatal("body-authored tag changed")
			}
			owner, err := p.fields(b)
			if err != nil || !strings.Contains(owner[0].Tag, `json:"updated"`) {
				t.Fatalf("owner tag changed %+v %v", owner, err)
			}
		})
	}
}

func TestOrdinarySharedAuxiliaryBodyOwnership(t *testing.T) {
	p, owner, current := ordinaryBodyPlanner()
	root := &spec.View{Name: "Scope", Auxiliary: true, Relations: []*spec.Relation{{View: owner}}}
	owner.Relations = []*spec.Relation{{Holder: "Children", View: &spec.View{Name: "Children", Source: &spec.ViewSource{Table: "children"}}}}
	p.names[owner.Relations[0].View] = "Child"
	p.ordinaryGraph = ordinaryBodyGraph(root)
	if err := p.shareOrdinaryBodyRow(current, owner, "record.go"); err != nil {
		t.Fatal(err)
	}
	// Merely adding a body relation to an unrelated row does not admit sharing.
	p.ordinaryGraph = ordinaryBodyGraph(&spec.View{Name: "Other", Auxiliary: true})
	if err := p.shareOrdinaryBodyRow(current, owner, "record.go"); err == nil {
		t.Fatal("unreachable owner admitted")
	}
	if !ordinaryBodyDeleteMarker(root) {
		t.Fatal("writable descendant delete marker not found")
	}
	if HasWritableDeleteMarker(root) {
		t.Fatal("global DELETE eligibility was broadened")
	}
	owner.Auxiliary = true
	if ordinaryBodyDeleteMarker(root) {
		t.Fatal("auxiliary marker used as writable witness")
	}
}

func TestOrdinarySharedBodyEmptyJSONNamePolicy(t *testing.T) {
	for _, tc := range []struct {
		name, body, read, explicitRead string
		want                           bool
	}{
		{"absent owner name", "", `json:"channels"`, "", true},
		{"absent owner with inferred read omission", "", `json:"channels,omitempty"`, "", true},
		{"absent owner keeps other options strict", "", `json:"channels,string"`, "", false},
		{"absent owner rejects explicit read", "", `json:"channels"`, `json:"channels"`, false},
		{"inferred name without omission", `json:",omitempty"`, `json:"channels"`, "", true},
		{"inferred name with omission", `json:",omitempty"`, `json:"channels,omitempty"`, "", true},
		{"explicit non-omission", `json:""`, `json:"channels,omitempty"`, "", true},
		{"other option retained", `json:",omitempty"`, `json:"channels,string,omitempty"`, "", false},
		{"explicit read stays strict", `json:",omitempty"`, `json:"channels"`, `json:"channels"`, false},
		{"nonempty owner rename stays strict", `json:"other,omitempty"`, `json:"channels,omitempty"`, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := ordinaryBodyColumn{column: &spec.Column{Tag: tc.body}, field: Field{Name: "Channels", Tag: tc.body}}
			read := ordinaryBodyColumn{column: &spec.Column{Tag: tc.explicitRead}, field: Field{Name: "Channels", Tag: tc.read}}
			if got := ordinaryBodyTags(body, read); got != tc.want {
				t.Fatalf("tag compatibility=%v want %v", got, tc.want)
			}
		})
	}
}
