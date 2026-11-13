package transcribe

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	tcolumn "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
	"reflect"
	"testing"
)

func TestGeneratedNestedWireSchemaRoundTrip(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	const module = "github.com/viant/datly/testfixture/nestedwire"
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	source := &Source{Name: "Records", Scope: module + "/records", Connector: "main", ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": db.DB}), Text: `#package('records')
#setting($_ = $route('/records','GET'))
#define($_ = $Day<[]string>(query/day).Optional().WithTag('wireSchemas:"{\"Day\":{\"minItems\":1,\"maxItems\":50,\"items\":{\"required\":true,\"pattern\":\".+\"}}}"'))
#define($_ = $Data<[]*Row>(output/view))
SELECT r.*,type(r,'Row') FROM records r`}
	previousTag := ""
	for i := 0; i < 2; i++ {
		generated, err := (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, field := range generated.Result.Plan.Input.Fields {
			if field.Name == "Day" {
				if i > 0 && previousTag != field.Tag {
					t.Fatalf("regeneration changed authored metadata: %q => %q", previousTag, field.Tag)
				}
				previousTag = field.Tag
				var schemas map[string]*spec.WireSchema
				if err = json.Unmarshal([]byte(reflect.StructTag(field.Tag).Get("wireSchemas")), &schemas); err != nil {
					t.Fatal(err)
				}
				if schema := schemas["Day"]; schema == nil || schema.MinItems != 1 || schema.MaxItems != 50 || schema.Items == nil || !schema.Items.Required || schema.Items.Pattern != ".+" {
					t.Fatalf("nested metadata lost: %+v", schemas)
				}
				found = true
			}
		}
		if !found {
			t.Fatal("generatedDaymissing")
		}
		holders, err := bootstrap.DiscoverComponentsFromPackages(ctx, root, []string{module + "/records"}, nil)
		if err != nil || len(holders) != 1 {
			t.Fatal("linkedholder", err)
		}
		component, err := holders[0].Resolve(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		input, _, err := generated.Types.Resolve(typecatalog.TranscribeAuthority, generated.Package.PkgPath+"."+generated.Result.Plan.Input.Type)
		if err != nil {
			t.Fatal(err)
		}
		output, _, err := generated.Types.Resolve(typecatalog.TranscribeAuthority, generated.Package.PkgPath+"."+generated.Result.Plan.Output.Type)
		if err != nil {
			t.Fatal(err)
		}
		resolver, err := typecatalog.NewResolver(generated.Types, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{PackagePath: generated.Package.PkgPath})
		if err != nil {
			t.Fatal(err)
		}
		resolved, err := (bootstrap.ContractResolver{Component: component, InputType: input, OutputType: output, Types: resolver}).Resolve()
		if err != nil {
			t.Fatal(err)
		}
		found = false
		for _, p := range resolved.Parameters {
			if p.Name == "Day" {
				found = true
				if schema := p.WireSchemas["Day"]; schema == nil || schema.MaxItems != 50 || schema.Items == nil || schema.Items.Pattern != ".+" {
					t.Fatalf("linked nested metadata lost: %+v", p.WireSchemas)
				}
			}
		}
		if !found {
			t.Fatal("linkedDaymissing")
		}
	}
}
