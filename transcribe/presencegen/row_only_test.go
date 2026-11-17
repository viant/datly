package presencegen

import (
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"strings"
	"testing"
)

func TestRowOnlyCompositeIdentityCompiles(t *testing.T) {
	source := "package fixture\nimport \"time\"\n" + `
type Row struct {
 Account int ` + "`sqlx:\"ACCOUNT,primaryKey=true\"`" + `
 At time.Time ` + "`sqlx:\"AT,primaryKey=true\"`" + `
 Value *string
 Codes []int
 Remove bool ` + "`sqlx:\"-\" writer:\"delete\"`" + `
 Version int ` + "`sqlx:\"VERSION\" writer:\"concurrency\"`" + `
 Proxy string ` + "`sqlx:\"PROXY,transient=true\"`" + `
 Child *Child ` + "`view:\"Child\"`" + `
 Children []*Child ` + "`view:\"Children\"`" + `
 Has *RowHas ` + "`setMarker:\"true\"`" + `
}
type RowHas struct { Account, At, Value, Codes, Remove, Version, Proxy, Child, Children bool }
type Child struct { ID int; Has *ChildHas ` + "`setMarker:\"true\"`" + ` }
type ChildHas struct { ID bool }
`
	generated, err := Generate("fixture", map[string][]byte{"shape.go": []byte(source)}, []string{"Row", "Child", "Child"}, Options{RowOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(generated), "PresenceIdentity") || strings.Contains(string(generated), "PresenceChanged") {
		t.Fatal("row-only generation emitted unused relation identity helpers")
	}
	if strings.Count(string(generated), "func (c *Child) SynchronizePresentMarker") != 1 {
		t.Fatal("shared receiver must own exactly one method")
	}
	fset := token.NewFileSet()
	var files []*ast.File
	for name, content := range map[string]string{"shape.go": source, "presence.go": string(generated)} {
		file, err := parser.ParseFile(fset, name, content, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
	}
	config := types.Config{Importer: importer.Default()}
	if _, err := config.Check("fixture", fset, files, nil); err != nil {
		t.Fatalf("generated source does not compile: %v\n%s", err, generated)
	}
}
