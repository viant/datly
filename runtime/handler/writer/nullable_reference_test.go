package writer

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
)

type nullableReferenceHas struct{ ID, ScopeID, ParentID, OwnerID bool }
type nullableReferenceRow struct {
	ID       int                   `sqlx:"id,primaryKey"`
	ScopeID  *string               `sqlx:"scope_id,required=true,refTable=targets,refColumn=scope_id" refKey:"scope_id,parent_id"`
	ParentID *string               `sqlx:"parent_id,refTable=targets,refColumn=parent_id" refKey:"scope_id,parent_id"`
	OwnerID  *string               `sqlx:"owner_id,refTable=owners,refColumn=id"`
	Has      *nullableReferenceHas `setMarker:"true" sqlx:"-"`
}
type nullableReferenceInput struct {
	Rows []*nullableReferenceRow `parameter:"Rows,kind=body,in=Data" view:"Rows,table=children"`
}
type nullableReferenceOutput struct{ Data []*nullableReferenceRow }

func TestNullableCompositeReferencePreservesDatabaseContract(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		scope, parent, owner *string
		seed                 bool
		wantError            string
	}{
		{name: "null member without any parent", scope: ptr("empty")},
		{name: "complete matching tuple", scope: ptr("one"), parent: ptr("first"), seed: true},
		{name: "cross tuple rejected", scope: ptr("one"), parent: ptr("second"), seed: true, wantError: "FOREIGN KEY"},
		{name: "unrelated reference remains checked", scope: ptr("empty"), owner: ptr("absent"), wantError: "OwnerID"},
		{name: "required field remains checked", wantError: "ScopeID"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := sqlite.New(t)
			for _, sql := range []string{
				"PRAGMA foreign_keys=ON",
				"CREATE TABLE targets(scope_id TEXT NOT NULL,parent_id TEXT NOT NULL,PRIMARY KEY(scope_id,parent_id))",
				"CREATE TABLE owners(id TEXT PRIMARY KEY)",
				"CREATE TABLE children(id INTEGER PRIMARY KEY,scope_id TEXT NOT NULL,parent_id TEXT,owner_id TEXT,FOREIGN KEY(scope_id,parent_id) REFERENCES targets(scope_id,parent_id),FOREIGN KEY(owner_id) REFERENCES owners(id))",
			} {
				if _, err := h.DB.Exec(sql); err != nil {
					t.Fatal(err)
				}
			}
			if tc.seed {
				if _, err := h.DB.Exec("INSERT INTO targets VALUES('one','first'),('two','second')"); err != nil {
					t.Fatal(err)
				}
			}
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[nullableReferenceInput]().PkgPath(), Name: "Rows"}, Settings: &spec.Settings{Mutation: "post"}, RootView: &spec.View{Name: "Rows", Source: &spec.ViewSource{Table: "children"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "scope_id", Source: "scope_id"}, {Name: "parent_id", Source: "parent_id"}, {Name: "owner_id", Source: "owner_id"}}}}
			input := &nullableReferenceInput{Rows: []*nullableReferenceRow{{ID: 1, ScopeID: tc.scope, ParentID: tc.parent, OwnerID: tc.owner}}}
			_, err := runSQLiteWriter(t, context.Background(), h, component, input, &nullableReferenceOutput{}, "post")
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("error=%v want %s", err, tc.wantError)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			var count int
			if e := h.DB.QueryRow("SELECT COUNT(*) FROM children").Scan(&count); e != nil {
				t.Fatal(e)
			}
			want := 1
			if tc.wantError != "" {
				want = 0
			}
			if count != want {
				t.Fatalf("persisted=%d want%d", count, want)
			}
		})
	}
}
