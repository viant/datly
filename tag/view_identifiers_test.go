package tag

import "testing"

func TestViewTableSQLIdentifierRoundtrip(t *testing.T) {
	for _, table := range []string{"`my-project.dataset.parents`", "[project.dataset.parents]", "[project:dataset.parents]", `"main"."odd.table"`, "[odd name]"} {
		value, err := (View{Name: "Records", Table: table}).Value()
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := ParseView(value)
		if err != nil || parsed.Table != table {
			t.Fatalf("%s -> %+v, %v", value, parsed, err)
		}
	}
	parsed, err := ParseView("Records,table=`p.d.t`,connector=\"main\",orderBy='id,name'")
	if err != nil || parsed.Table != "`p.d.t`" || parsed.Connector != "main" || parsed.OrderBy != "id,name" {
		t.Fatalf("unrelated options changed: %+v %v", parsed, err)
	}
	if _, err := (View{Name: "Records", Table: "bad\ntable"}).Value(); err == nil {
		t.Fatal("newline accepted")
	}
	if _, err := (View{Name: "Records", EntityHooks: "`Hooks`"}).Value(); err == nil {
		t.Fatal("non-table delimiter check weakened")
	}
}
