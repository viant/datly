package writer

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
)

type tagPolicyMembership struct {
	OwnerID int  `sqlx:"owner_id,primaryKey=true"`
	RailID  int  `sqlx:"rail_id,primaryKey=true"`
	Remove  bool `sqlx:"-" writer:"delete"`
}
type tagPolicyOwner struct {
	ID      int                    `sqlx:"id,primaryKey=true"`
	Members []*tagPolicyMembership `view:"Members,table=members,writerActionPolicy=insert-delete" on:"ID=OwnerID"`
}
type tagPolicyInput struct {
	Rows           []*tagPolicyOwner      `parameter:"Rows,kind=body,in=data" view:"Rows,table=owners"`
	CurrentRows    []*tagPolicyOwner      `parameter:"CurrentRows,kind=view" view:"CurrentRows,table=owners"`
	CurrentMembers []*tagPolicyMembership `parameter:"CurrentMembers,kind=view" view:"CurrentMembers,table=members"`
}
type tagPolicyOutput struct {
	Data []*tagPolicyOwner `parameter:"Data,kind=output,in=body"`
}

func TestNestedGoTagInsertDeleteKeepsDatabaseGeneratedIdentity(t *testing.T) {
	for _, reject := range []bool{false, true} {
		name := "replace"
		if reject {
			name = "rollback"
		}
		t.Run(name, func(t *testing.T) {
			db := sqlite.New(t)
			ctx := context.Background()
			if err := db.ExecStatements(ctx,
				"CREATE TABLE owners(id INTEGER PRIMARY KEY)",
				"CREATE TABLE members(id INTEGER PRIMARY KEY AUTOINCREMENT,owner_id INTEGER NOT NULL,rail_id INTEGER NOT NULL,UNIQUE(owner_id,rail_id))",
				"INSERT INTO owners VALUES(1),(2)",
				"INSERT INTO members(owner_id,rail_id) VALUES(1,10),(2,10)"); err != nil {
				t.Fatal(err)
			}
			if reject {
				if err := db.ExecStatements(ctx, "CREATE TRIGGER reject_member BEFORE INSERT ON members WHEN NEW.owner_id=1 BEGIN SELECT RAISE(ABORT,'rejected replacement'); END"); err != nil {
					t.Fatal(err)
				}
			}
			// Child policy is carried by the generated Go relation tag, with no duplicate
			// child spec. The surrogate ID is intentionally absent from the write shape.
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[tagPolicyOwner]().PkgPath(), Name: "Rows"}, Settings: &spec.Settings{Mutation: "patch"}, RootView: &spec.View{Name: "Rows", Source: &spec.ViewSource{Table: "owners"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}}}}
			in := &tagPolicyInput{Rows: []*tagPolicyOwner{{ID: 1, Members: []*tagPolicyMembership{{OwnerID: 1, RailID: 10, Remove: true}, {OwnerID: 1, RailID: 10}}}}, CurrentRows: []*tagPolicyOwner{{ID: 1}}, CurrentMembers: []*tagPolicyMembership{{OwnerID: 1, RailID: 10}}}
			_, err := runSQLiteWriter(t, ctx, db, component, in, &tagPolicyOutput{}, "patch")
			if (err != nil) != reject {
				t.Fatalf("reject=%v err=%v", reject, err)
			}
			var newID, siblingID int
			if err := db.DB.QueryRow("SELECT id FROM members WHERE owner_id=1 AND rail_id=10").Scan(&newID); err != nil {
				t.Fatal(err)
			}
			if reject && newID != 1 {
				t.Fatalf("rollback changed identity: %d", newID)
			}
			if !reject && newID <= 2 {
				t.Fatalf("database did not assign a new identity: %d", newID)
			}
			if err := db.DB.QueryRow("SELECT id FROM members WHERE owner_id=2 AND rail_id=10").Scan(&siblingID); err != nil || siblingID != 2 {
				t.Fatalf("sibling=%d err=%v", siblingID, err)
			}
		})
	}
}
