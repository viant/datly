package generate

import (
	"encoding/json"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	"reflect"
	"strings"
	"testing"
	"time"
)

type linkedDateAliasRow struct {
	StartDate    *time.Time `sqlx:"START_DATE"`
	StartDateUtc *string    `sqlx:"START_DATE_UTC" source:"START_DATE"`
}
type linkedDateAliasDuplicate struct {
	StartDate *time.Time `sqlx:"START_DATE"`
	First     *string    `sqlx:"START_DATE_UTC"`
	Second    *string    `sqlx:"START_DATE_UTC"`
}

func TestLinkedCASTPrefersSQLResultAliasOverSharedPhysicalSource(t *testing.T) {
	for _, tc := range []struct {
		name    string
		row     any
		column  *spec.Column
		failure string
	}{
		{name: "original date alias", row: linkedDateAliasRow{}, column: &spec.Column{Name: "START_DATE_UTC", Source: "START_DATE", ExplicitType: true, Type: spec.TypeRef{Name: "string", Pointer: true}}},
		{name: "alias mismatch", row: linkedDateAliasRow{}, column: &spec.Column{Name: "START_DATE_UTC", Source: "START_DATE", ExplicitType: true, Type: spec.TypeRef{Name: "int", Pointer: true}}, failure: "uneditable linked field"},
		{name: "duplicate alias", row: linkedDateAliasDuplicate{}, column: &spec.Column{Name: "START_DATE_UTC", Source: "START_DATE", ExplicitType: true, Type: spec.TypeRef{Name: "string", Pointer: true}}, failure: "ambiguous SQLX"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := typecatalog.NewCatalog()
			descriptor := x.NewType(reflect.TypeOf(tc.row))
			if err := catalog.Register(typecatalog.TypeOriginPackage, descriptor); err != nil {
				t.Fatal(err)
			}
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, nil)
			if err != nil {
				t.Fatal(err)
			}
			view := &spec.View{Name: "Linked", Columns: []*spec.Column{tc.column}}
			before, _ := json.Marshal(view)
			identity, err := view.Identity()
			if err != nil {
				t.Fatal(err)
			}
			_, err = New(Input{Component: &spec.Component{Name: "Records", RootView: &spec.View{Name: "Root"}, Views: []*spec.View{view}}, TypeResolver: resolver, Views: ViewReferences{identity: &ViewReference{DescriptorKey: descriptor.Key()}}}).Plan()
			if tc.failure == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.failure) {
				t.Fatalf("error=%v", err)
			}
			after, _ := json.Marshal(view)
			if string(before) != string(after) {
				t.Fatal("linked field annotations changed")
			}
		})
	}
}
