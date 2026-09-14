package transcribe

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	tcolumn "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
)

// Exercise authored non-body inputs as well as the mutation payload. Selecting
// the payload for DML must not replace the complete component input contract.
func TestMutationGenerationPreservesDQLInputContract(t *testing.T) {
	for _, target := range []HandlerTarget{HandlerGo, HandlerVelty} {
		for _, operation := range []WriteOperation{WritePatch, WritePost, WritePut} {
			t.Run(string(target)+"/"+string(operation), func(t *testing.T) {
				harness := testharness.NewSQLiteHarness(t)
				ctx := context.Background()
				if err := harness.ExecStatements(ctx, `CREATE TABLE RECORDS (ID INTEGER PRIMARY KEY, NAME TEXT)`); err != nil {
					t.Fatal(err)
				}
				root := t.TempDir()
				testharness.WriteGeneratedGoMod(t, root)
				current, declaration := "", ""
				if operation == WritePatch {
					current = "CurrentRecords"
					declaration = "#define($_ = $CurrentRecords<?>(view/CurrentRecords).Cardinality('Many') /* SELECT ID, NAME FROM RECORDS */)\n"
				}
				dql := fmt.Sprintf(`#setting($_ = $route('/records', '%s'))
#setting($_ = $input_type('MutationRequest'))
#define($_ = $Records<[]*RecordsView>(body/Data).Cardinality('Many').Required())
#define($_ = $RequestID<string>(header/X-Request-ID).Required())
#define($_ = $DryRun<bool>(query/dryRun))
%s#define($_ = $Data<[]*RecordsView>(output/body))
SELECT ID, NAME FROM RECORDS`, strings.ToUpper(string(operation)), declaration)
				generated, err := NewCompiler().Transcribe(ctx, Request{
					Source: &Source{Scope: "example.com/contracts/records", Name: "Records", Connector: "main", Text: dql,
						Types: typecatalog.NewCatalog(), ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": harness.DB})},
					Destination: root,
					Options:     Options{Handler: HandlerOptions{Target: target, Operation: operation, Current: current}},
				})
				if err != nil {
					t.Fatal(err)
				}
				input := &generated.Result.Plan.Input
				if input.Type != "MutationRequest" {
					t.Fatalf("component input = %q, want authored MutationRequest", input.Type)
				}
				const importPath = "example.com/generated/generated"
				if generated.Package.PkgPath != importPath {
					t.Fatalf("generated package = %q, want module-qualified %q", generated.Package.PkgPath, importPath)
				}
				descriptor, found, err := generated.Types.Resolve(typecatalog.PackageAuthority, importPath+".MutationRequest")
				if err != nil || !found || descriptor == nil || descriptor.PkgPath != importPath {
					t.Fatalf("generated input package authority = %+v, found=%v, error=%v", descriptor, found, err)
				}
				var holderFound bool
				for _, file := range generated.Result.Files {
					if strings.Contains(file.Content, "xdatly.Component[MutationRequest,") {
						holderFound = true
					}
				}
				if !holderFound {
					t.Fatal("generated component holder does not use the authored input contract")
				}
				for _, expected := range []struct{ name, binding string }{
					{"Records", "body"}, {"RequestID", "X-Request-ID"}, {"DryRun", "dryRun"},
				} {
					field, ok := input.Field(expected.name)
					if !ok || !strings.Contains(field.Tag, expected.binding) {
						t.Fatalf("authored input %s lost: %+v", expected.name, field)
					}
				}
				if current != "" {
					if _, ok := input.Field(current); !ok {
						t.Fatal("PATCH current-state input lost")
					}
				}
			})
		}
	}
}
