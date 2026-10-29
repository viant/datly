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
	"strings"
	"testing"
)

func TestGeneratedEnumContractReload(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	const module = "github.com/viant/datly/testfixture/enumreader"
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	source := &Source{Name: "Records", Scope: module + "/records", Connector: "main", ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": db.DB}), Text: `#package('records')
#setting($_ = $route('/records','GET'))
#define($_ = $Mode<int64>(query/mode).Optional().Value(9007199254740993).WithEnum(9007199254740993,9007199254740994))
#define($_ = $Order<int>(query/order).Optional().Value(3).WithEnum(3,1,2))
#define($_ = $Text<string>(query/text).Optional().WithEnum("z","a","m"))
#define($_ = $NumericText<string>(query/numericText).Optional().WithEnum("3","1","2"))
#define($_ = $Flag<*bool>(query/flag).Optional().WithEnum(false,true))
#define($_ = $Data<[]*Row>(output/view))
SELECT r.*, type(r,'Row') FROM records r`}
	for i := 0; i < 3; i++ {
		if i == 2 {
			source.Text = strings.Replace(source.Text, ".WithEnum(9007199254740993,9007199254740994)", "", 1)
		}
		generated, err := (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
		if err != nil {
			t.Fatal(err)
		}
		holders, err := bootstrap.DiscoverComponentsFromPackages(ctx, root, []string{module + "/records"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(holders) != 1 {
			t.Fatal(len(holders))
		}
		input, found, err := generated.Types.Resolve(typecatalog.TranscribeAuthority, generated.Package.PkgPath+"."+generated.Result.Plan.Input.Type)
		if err != nil || !found {
			t.Fatalf("input: %v", err)
		}
		output, found, err := generated.Types.Resolve(typecatalog.TranscribeAuthority, generated.Package.PkgPath+"."+generated.Result.Plan.Output.Type)
		if err != nil || !found {
			t.Fatalf("output: %v", err)
		}
		resolver, err := typecatalog.NewResolver(generated.Types, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{PackagePath: generated.Package.PkgPath})
		if err != nil {
			t.Fatal(err)
		}
		canonical, err := holders[0].Resolve(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		resolved, err := (bootstrap.ContractResolver{Component: canonical, InputType: input, OutputType: output, Types: resolver}).Resolve()
		if err != nil {
			t.Fatal(err)
		}
		found = false
		wants := map[string]string{"Mode": "[9007199254740993,9007199254740994]", "Order": "[3,1,2]", "Text": `["z","a","m"]`, "NumericText": `["3","1","2"]`, "Flag": "[false,true]"}
		for _, parameter := range resolved.Parameters {
			if parameter.Name == "Mode" && i == 2 {
				found = true
				delete(wants, "Mode")
				if parameter.WireSchema != nil && len(parameter.WireSchema.Enum) > 0 {
					t.Fatal("generator-owned choice metadata was not removed with its source declaration")
				}
				continue
			}
			if want, ok := wants[parameter.Name]; ok {
				if parameter.WireSchema == nil {
					t.Fatalf("round%d %s choice metadata missing; tag=%s", i, parameter.Name, parameter.Tag)
				}
				b, _ := json.Marshal(parameter.WireSchema.Enum)
				if string(b) != want {
					t.Fatalf("%s exact ordered wire JSON=%s want%s", parameter.Name, b, want)
				}
				delete(wants, parameter.Name)
			}
			if parameter.Name == "Mode" {
				found = true
				// Generated Go reload must retain exact typed values and wire digits.
				if parameter.WireSchema == nil || len(parameter.WireSchema.Enum) != 2 || parameter.WireSchema.Enum[0] != int64(9007199254740993) {
					t.Fatalf("generated enum lost exact domain: %+v", parameter.WireSchema)
				}
			}
		}
		if len(wants) > 0 {
			t.Fatal(wants)
		}
		if !found {
			t.Fatal("mode missing")
		}
	}
}

func TestGeneratedNonEnumWireSchemaRoundTrip(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	const module = "github.com/viant/datly/testfixture/nonenumwire"
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	source := &Source{Name: "Records", Scope: module + "/records", Connector: "main", ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": db.DB}), Text: `#package('records')
#setting($_ = $route('/records','GET'))
#define($_ = $Day<string>(query/day).Optional().Tag('wireSchema:"{\"type\":\"string\",\"format\":\"date\",\"nullable\":true}"'))
#define($_ = $Data<[]*Row>(output/view))
SELECT r.*,type(r,'Row') FROM records r`}
	for i := 0; i < 2; i++ {
		generated, err := (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, field := range generated.Result.Plan.Input.Fields {
			if field.Name == "Day" {
				var schema spec.WireSchema
				if err = json.Unmarshal([]byte(reflect.StructTag(field.Tag).Get("wireSchema")), &schema); err != nil {
					t.Fatal(err)
				}
				if schema.Type != "string" || schema.Format != "date" || !schema.Nullable || len(schema.Enum) != 0 {
					t.Fatalf("nonenum metadata lost: %+v", schema)
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
				if p.WireSchema == nil || p.WireSchema.Format != "date" || !p.WireSchema.Nullable || len(p.WireSchema.Enum) != 0 {
					t.Fatalf("linked nonenum metadata lost: %+v", p.WireSchema)
				}
			}
		}
		if !found {
			t.Fatal("linkedDaymissing")
		}
	}
}
