package presencegen

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

type namedComparisonBytes []byte

func TestGeneratedNamedSliceEquality(t *testing.T) {
	for _, rowOnly := range []bool{false, true} {
		for _, kind := range []string{"local definition", "local alias", "imported alias", "catalog named slice"} {
			t.Run(fmt.Sprintf("rowOnly=%v/%s", rowOnly, kind), func(t *testing.T) {
				declarations := "type Payload []byte\ntype Alias = Payload\ntype Optional = *Alias\n"
				options := Options{RowOnly: rowOnly}
				extra := map[string]string{}
				switch kind {
				case "local alias":
					declarations = "type Payload = []byte\ntype Alias = Payload\ntype Optional = *Alias\n"
				case "imported alias":
					declarations = "import wire \"encoding/json\"\ntype Payload = wire.RawMessage\ntype Alias = Payload\ntype Optional = *Alias\n"
				case "catalog named slice":
					declarations = "import wire \"fixture/contract\"\ntype Payload = wire.Bytes\ntype Alias = Payload\ntype Optional = *Alias\n"
					extra["contract/bytes.go"] = "package contract\ntype Bytes []byte\n"
					catalog := typecatalog.NewCatalog()
					if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[namedComparisonBytes](), x.WithPkgPath("fixture/contract"), x.WithName("Bytes"))); err != nil {
						t.Fatal(err)
					}
					var err error
					options.TypeResolver, err = typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
					if err != nil {
						t.Fatal(err)
					}
				}
				source := strings.Replace(rawMessageShape, "import JSON \"encoding/json\"", declarations, 1)
				source = strings.ReplaceAll(source, "*JSON.RawMessage", "Optional")
				source = strings.ReplaceAll(source, "JSON.RawMessage", "Alias")
				checks := strings.ReplaceAll(rawMessageChecks, " \"encoding/json\"\n", "")
				checks = strings.ReplaceAll(checks, "json.RawMessage", "Payload")
				generated, err := Generate("fixture", map[string][]byte{"shape.go": []byte(source)}, []string{"Row"}, options)
				if err != nil {
					t.Fatal(err)
				}
				dir := t.TempDir()
				extra["go.mod"] = "module fixture\n\ngo 1.23\n"
				extra["shape.go"], extra["presence.go"], extra["presence_test.go"] = source, string(generated), checks
				for name, data := range extra {
					path := filepath.Join(dir, name)
					if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, []byte(data), 0600); err != nil {
						t.Fatal(err)
					}
				}
				command := exec.Command("go", "test", "-count=1", ".")
				command.Dir = dir
				command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
				if output, err := command.CombinedOutput(); err != nil {
					t.Fatalf("generated comparison failed: %v\n%s\n%s", err, output, generated)
				}
			})
		}
	}
}

func TestUnsupportedNamedScalarFailsGeneration(t *testing.T) {
	for _, definition := range []string{
		"type Payload [][]byte", "type Payload map[string]string", "type Payload []map[string]string",
		"type Payload struct { Values []byte }", "type Payload = Alias; type Alias = Payload",
	} {
		for _, pointer := range []string{"", "*"} {
			t.Run(definition+pointer, func(t *testing.T) {
				source := "package fixture\n" + definition + "\ntype Row struct { Value " + pointer + "Payload; Has *RowHas `setMarker:\"true\"` }; type RowHas struct { Value bool }"
				_, err := Generate("fixture", map[string][]byte{"shape.go": []byte(source)}, []string{"Row"}, Options{RowOnly: true})
				if err == nil || !strings.Contains(err.Error(), "Row.Value has unsupported marked type") {
					t.Fatalf("expected field-specific generation error, got %v", err)
				}
			})
		}
	}
}
