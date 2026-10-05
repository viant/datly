package generate

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/compile"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestLocalTypeEmissionDescriptorAuthority(t *testing.T) {
	const local = "example.com/localtypes/model"
	const external = "example.com/elsewhere/model"
	for _, tc := range []struct {
		name, authored, pkg, want string
	}{
		{"local alias", "self.Value", local, "Value"},
		{"local pointer", "*self.Value", local, "*Value"},
		{"local slice pointers", "[]*self.Value", local, "[]*Value"},
		{"local pointer slice", "*[]self.Value", local, "*[]Value"},
		{"local full path", "[]*" + local + ".Value", local, "[]*Value"},
		{"external same basename", "other.Value", external, "other.Value"},
		{"external pointer", "*other.Value", external, "*other.Value"},
		{"external slice pointers", "[]*other.Value", external, "[]*other.Value"},
		{"external full path", "[]*" + external + ".Value", external, "[]*model.Value"},
		{"unknown package preserves authored alias", "self.Value", "", "self.Value"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := &Plan{Package: local}
			resolver := fieldTypeResolver{
				plan: plan, targetPackage: local,
				context: &spec.TypeContext{DefaultPackage: local, Imports: []spec.ImportSpec{{Alias: "self", Package: local}, {Alias: "other", Package: external}}},
				lookup:  func(string) (*x.Type, error) { return &x.Type{Name: "Value", PkgPath: tc.pkg}, nil },
			}
			actual, err := resolver.resolve(tc.authored)
			if err != nil || actual != tc.want {
				t.Fatalf("resolve(%q) = %q, %v; want %q", tc.authored, actual, err, tc.want)
			}
			if tc.pkg == local && len(plan.Imports) != 0 {
				t.Fatalf("local descriptor added imports: %+v", plan.Imports)
			}
			alias := "other"
			if tc.name == "external full path" {
				alias = "model"
			}
			if tc.pkg == external && !reflect.DeepEqual(plan.Imports, []spec.ImportSpec{{Alias: alias, Package: external}}) {
				t.Fatalf("external alias changed: %+v", plan.Imports)
			}
			if tc.pkg == "" && !reflect.DeepEqual(plan.Imports, []spec.ImportSpec{{Alias: "self", Package: local}}) {
				t.Fatalf("unknown identity changed existing alias behavior: %+v", plan.Imports)
			}
		})
	}
	// Unknown identities are not proof of locality. Preserve the old empty
	// descriptor/name behavior separately from the qualified alias branch.
	resolver := fieldTypeResolver{plan: &Plan{}, targetPackage: local}
	for _, descriptor := range []*x.Type{nil, {Name: ""}, {Name: " "}} {
		if actual := resolver.emittedNamedType("self.Value", descriptor); actual != "" {
			t.Fatalf("empty descriptor emitted %q", actual)
		}
	}
	if actual := resolver.emittedNamedType("Value", &x.Type{Name: "Value"}); actual != "Value" {
		t.Fatalf("unqualified unknown identity = %q", actual)
	}
	lookupFailure := errors.New("descriptor authority unavailable")
	resolver.lookup = func(string) (*x.Type, error) { return nil, lookupFailure }
	if _, err := resolver.resolve("[]*self.Value"); err == nil || !strings.Contains(err.Error(), lookupFailure.Error()) {
		t.Fatalf("lookup failure was lost: %v", err)
	}
	resolver.lookup = func(string) (*x.Type, error) { return nil, nil }
	if _, err := resolver.resolve("self.Value"); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("missing descriptor accepted: %v", err)
	}
	resolver.context = &spec.TypeContext{DefaultPackage: external}
	resolver.plan.RootViewType = "Record"
	resolver.lookup = func(string) (*x.Type, error) {
		t.Fatal("generated local view reached external lookup")
		return nil, nil
	}
	if actual, err := resolver.resolve("[]*Record"); err != nil || actual != "[]*Record" {
		t.Fatalf("local generated view = %q, %v", actual, err)
	}
}

func TestLocalTypeEmissionScalarAndMapScaffold(t *testing.T) {
	const local = "example.com/localtypes/model"
	const external = "example.com/localtypes/elsewhere/model"
	imports := []spec.ImportSpec{{Alias: "self", Package: local}, {Alias: "other", Package: external}}
	root := localEmissionModule(t)
	localEmissionWrite(t, root, "model/authority.go", "package model\ntype Key string\ntype Value struct { Label string }\n")
	localEmissionWrite(t, root, "elsewhere/model/authority.go", "package model\ntype Key string\ntype Value struct { Number int }\n")
	plan := &Plan{Package: local, GoPackage: "model", ComponentName: "Emission", ProjectRoot: root, ShapesOnly: true, Generation: &spec.GenerationSettings{}, RouterDest: "router.go", ViewDest: "rows.go", Imports: append([]spec.ImportSpec(nil), imports...)}
	for i, tc := range []struct {
		name, want string
		typ        spec.TypeRef
	}{
		{"local scalar", "*Value", spec.TypeRef{Package: local, Name: "Value", Pointer: true}},
		{"local scalar slice", "[]*Value", spec.TypeRef{Package: local, Name: "Value", Pointer: true, Cardinality: spec.CardinalityMany}},
		{"external same basename", "*other.Value", spec.TypeRef{Package: external, Name: "Value", Pointer: true}},
		{"local key full path", "map[Key][]*other.Value", spec.TypeRef{Name: "map[" + local + ".Key][]*" + external + ".Value"}},
		{"local value alias", "map[other.Key]*Value", spec.TypeRef{Name: "map[other.Key]*self.Value"}},
		{"both local aliases", "map[Key]*Value", spec.TypeRef{Name: "map[self.Key]*self.Value"}},
		{"nested mixed wrappers", "*map[Key]map[other.Key]*[]*Value", spec.TypeRef{Name: "map[self.Key]map[other.Key]*[]*self.Value", Pointer: true}},
		{"slice of maps", "[]map[Key][]*other.Value", spec.TypeRef{Name: "map[self.Key][]*other.Value", Cardinality: spec.CardinalityMany}},
		{"pointer slice of maps", "*[]map[Key]*Value", spec.TypeRef{Name: "map[self.Key]*self.Value", Cardinality: spec.CardinalityMany, SlicePointer: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := &spec.View{Name: "Record", Columns: []*spec.Column{{Name: "Payload", Source: "PAYLOAD", ExplicitType: true, Type: tc.typ, Tag: `json:"payload,omitempty" validate:"required"`}}}
			fields, err := resolveScalarViewFields(plan, view, false)
			if err != nil || len(fields) != 1 || fields[0].Type != tc.want {
				t.Fatalf("fields = %+v, %v; want %q", fields, err, tc.want)
			}
			if !fields[0].ExplicitType || !strings.Contains(fields[0].Tag, `json:"payload,omitempty"`) || !strings.Contains(fields[0].Tag, `validate:"required"`) || reflect.StructTag(fields[0].Tag).Get("sqlx") != "PAYLOAD" {
				t.Fatalf("column metadata changed: %+v", fields[0])
			}
			name := fmt.Sprintf("Record%d", i)
			plan.Views = append(plan.Views, ViewPlan{Name: name, Type: name, Package: local, Ownership: ViewGenerated, Destination: "rows.go", Fields: fields})
		})
	}
	files, err := EmitScaffold(filepath.Join(root, "model"), plan)
	if err != nil {
		t.Fatal(err)
	}
	localEmissionAssertSources(t, files)
	localEmissionWrite(t, root, "model/identity_test.go", `package model
import ("testing"; other "example.com/localtypes/elsewhere/model")
func TestNominalIdentity(t *testing.T) {
 var _ *Value = Record0{}.Payload
 var _ []*Value = Record1{}.Payload
 var _ *other.Value = Record2{}.Payload
 var _ map[Key][]*other.Value = Record3{}.Payload
 var _ map[other.Key]*Value = Record4{}.Payload
 var _ map[Key]*Value = Record5{}.Payload
 var _ *map[Key]map[other.Key]*[]*Value = Record6{}.Payload
 var _ []map[Key][]*other.Value = Record7{}.Payload
 var _ *[]map[Key]*Value = Record8{}.Payload
}
`)
	localEmissionCompile(t, root)
	// Unknown aliases and same basenames must never be guessed to be local.
	unknown := &Plan{Package: local}
	actual, err := emittedMapColumnType(unknown, "map[ghost.Key]*ghost.Value")
	if err != nil || actual != "map[ghost.Key]*ghost.Value" || !reflect.DeepEqual(unknown.Imports, []spec.ImportSpec{{Alias: "ghost", Package: "ghost"}}) {
		t.Fatalf("unknown map qualifier = %q, %v, %+v", actual, err, unknown.Imports)
	}
	collision := &Plan{Package: local}
	actual, err = emittedMapColumnType(collision, "map[example.com/first/model.Key]*example.com/second/model.Value")
	if err != nil || actual != "map[model.Key]*model2.Value" {
		t.Fatalf("external basename collision = %q, %v", actual, err)
	}
	if !reflect.DeepEqual(collision.Imports, []spec.ImportSpec{{Alias: "model", Package: "example.com/first/model"}, {Alias: "model2", Package: "example.com/second/model"}}) {
		t.Fatalf("colliding package identities changed: %+v", collision.Imports)
	}
}

func TestLocalTypeEmissionSQLDerivedNamedScalar(t *testing.T) {
	const local = "example.com/localtypes/model"
	context := &spec.TypeContext{DefaultPackage: local, Imports: []spec.ImportSpec{{Alias: "self", Package: local}}}
	for _, spelling := range []string{"*self.Value", "*Value"} {
		t.Run(spelling, func(t *testing.T) {
			SQL := "SELECT r.*, CAST(r.payload AS " + spelling + "), tag(r.payload,'json:\"payload\"') FROM records r"
			view, err := compile.NewReader().Compile(compile.ReadInput{View: &spec.View{Name: "Records"}, SQL: SQL, TypeContext: context})
			if err != nil {
				t.Fatal(err)
			}
			if len(view.Columns) != 1 || view.Columns[0].Type.Package != local || !view.Columns[0].ExplicitType || strings.Contains(strings.ToLower(view.Source.SQL), "cast(") {
				t.Fatalf("SQL declaration authority lost: %+v", view)
			}
			plan := &Plan{Package: local, Imports: context.Imports}
			fields, err := resolveScalarViewFields(plan, view, false)
			if err != nil || len(fields) != 1 || fields[0].Type != "*Value" {
				t.Fatalf("SQL-derived fields = %+v, %v", fields, err)
			}
			source := structFileWithImports("model", "// Record is SQL-derived.", "Record", fields, plan.Imports)
			root := localEmissionModule(t)
			localEmissionWrite(t, root, "model/authority.go", "package model\ntype Value struct { Label string }\n")
			localEmissionWrite(t, root, "model/record.go", source)
			localEmissionAssertSources(t, []EmittedFile{{Path: filepath.Join(root, "model/record.go"), Content: source}})
			localEmissionCompile(t, root)
		})
	}
}

func TestLocalTypeEmissionDestinationOwnership(t *testing.T) {
	const base = "example.com/localtypes/model"
	const dest = "example.com/localtypes/dto"
	for _, baseReference := range []bool{false, true} {
		t.Run(fmt.Sprintf("base_reference_%t", baseReference), func(t *testing.T) {
			root := localEmissionModule(t)
			localEmissionWrite(t, root, "model/authority.go", "package model\ntype Key string\ntype Value struct { Label string }\n")
			localEmissionWrite(t, root, "dto/authority.go", "package dto\ntype Key string\ntype Value struct { Number int }\n")
			imports := []spec.ImportSpec{{Alias: "self", Package: base}, {Alias: "dto", Package: dest}}
			context := &spec.TypeContext{DefaultPackage: base, Imports: imports}
			component := &spec.Component{Settings: &spec.Settings{InputType: "dto.Request", OutputType: "dto.Response"}, TypeContext: context, RootView: &spec.View{Name: "Records", TypeName: "dto.Record", Dest: "record.go"}}
			destinations := &shapeDestinations{input: &Input{Component: component, TargetPackage: base, ProjectRoot: root}}
			if err := destinations.prepare(); err != nil {
				t.Fatal(err)
			}
			plan := &Plan{Package: base, GoPackage: "model", ProjectRoot: root, ComponentName: "Relocation", Generation: &spec.GenerationSettings{}, ShapesOnly: true, RouterDest: "router.go", ViewDest: "record.go", RootViewType: "Record", Imports: append([]spec.ImportSpec(nil), imports...)}
			pkg, alias := dest, "dto"
			want := "Value"
			wantMap := "map[Key][]*Value"
			if baseReference {
				pkg, alias, want, wantMap = base, "self", "self.Value", "map[self.Key][]*self.Value"
			}
			resolver := fieldTypeResolver{plan: plan, context: context, targetPackage: base, lookup: func(string) (*x.Type, error) { return &x.Type{Name: "Value", PkgPath: pkg}, nil }}
			parameter, err := resolver.resolve("[]*" + alias + ".Value")
			if err != nil {
				t.Fatal(err)
			}
			plan.Input = ContractPlan{Type: "Request", Destination: "request.go", Ownership: ContractGenerated, Fields: []Field{{Name: "Values", Type: parameter}}}
			plan.Output = ContractPlan{Type: "Response", Destination: "response.go", Ownership: ContractGenerated, Fields: []Field{{Name: "Values", Type: parameter}}}
			fields, err := resolveScalarViewFields(plan, &spec.View{Name: "Records", Columns: []*spec.Column{{Name: "Value", ExplicitType: true, Type: spec.TypeRef{Package: pkg, Name: "Value", Pointer: true}}, {Name: "Values", ExplicitType: true, Type: spec.TypeRef{Name: "map[" + alias + ".Key][]*" + alias + ".Value"}}}}, false)
			if err != nil {
				t.Fatal(err)
			}
			plan.Views = []ViewPlan{{Name: "Record", Type: "Record", Identity: RootViewPath, Ownership: ViewGenerated, Destination: "record.go", Fields: fields}}
			if err = destinations.partition(plan); err != nil {
				t.Fatal(err)
			}
			if len(plan.ShapePackages) != 1 {
				t.Fatalf("destinations = %+v", plan.ShapePackages)
			}
			group := plan.ShapePackages[0]
			for name, key := range group.knownLocalTypes {
				if !strings.HasPrefix(key, dest+".") {
					t.Fatalf("base package descriptor metadata leaked to destination: %s=%s", name, key)
				}
			}
			if group.Package != dest || group.Input.Fields[0].Type != "[]*"+want || group.Output.Fields[0].Type != "[]*"+want || group.Views[0].Fields[0].Type != "*"+want || group.Views[0].Fields[1].Type != wantMap {
				t.Fatalf("relocated nominal identity: input=%+v views=%+v", group.Input, group.Views)
			}
			// Compile the actual relocated consuming scaffold against the original
			// base declarations. Whole-component aliases also form a real cycle
			// in the reverse direction, and must remain rejected below.
			files, err := EmitScaffold(filepath.Join(root, "dto"), group)
			if err != nil {
				t.Fatal(err)
			}
			localEmissionAssertSources(t, files)
			localEmissionCompile(t, root)
			if baseReference {
				if _, err = EmitScaffold(filepath.Join(root, "model"), plan); err == nil || !strings.Contains(err.Error(), "generation import cycle") {
					t.Fatalf("real base/destination cycle accepted: %v", err)
				}
			} else {
				files, err = EmitScaffold(filepath.Join(root, "model"), plan)
				if err != nil {
					t.Fatal(err)
				}
				localEmissionAssertSources(t, files)
				localEmissionCompile(t, root)
			}
			if count := localEmissionDeclarationCount(t, filepath.Join(root, "dto"), "Value"); count != 1 {
				t.Fatalf("existing destination Value declarations=%d", count)
			}
			if actual, err := os.ReadFile(filepath.Join(root, "dto/authority.go")); err != nil || string(actual) != "package dto\ntype Key string\ntype Value struct { Number int }\n" {
				t.Fatalf("existing destination authority changed: %s, %v", actual, err)
			}
		})
	}
}

type localEmissionDestinationKey string
type localEmissionDestinationValue struct{ Destination bool }
type localEmissionBaseKey string
type localEmissionForeignKey int
type localEmissionFourthValue struct{ Float float64 }

func TestLocalTypeEmissionRelocatedMixedMapIdentity(t *testing.T) {
	const base = "example.com/localtypes/model"
	const destination = "example.com/localtypes/destination/model"
	const third = "example.com/localtypes/third/model"
	const fourth = "example.com/localtypes/fourth/model"
	root := localEmissionModule(t)
	baseAuthority := "package model\ntype Key string\ntype Value struct { Label string }\n"
	destinationAuthority := "package model\ntype Key string\ntype Value struct { Destination bool }\n"
	thirdAuthority := "package model\ntype Key int\ntype Value struct { Number int }\n"
	fourthAuthority := "package model\ntype Value struct { Float float64 }\n"
	localEmissionWrite(t, root, "model/authority.go", baseAuthority)
	localEmissionWrite(t, root, "destination/model/authority.go", destinationAuthority)
	localEmissionWrite(t, root, "third/model/authority.go", thirdAuthority)
	localEmissionWrite(t, root, "fourth/model/authority.go", fourthAuthority)
	imports := []spec.ImportSpec{{Alias: "self", Package: base}, {Alias: "dto", Package: destination}, {Alias: "foreign", Package: third}, {Alias: "model", Package: base}}
	plan, destinations := localEmissionRelocationSetup(t, root, base, destination, imports)
	keyType, valueType := reflect.TypeFor[localEmissionDestinationKey](), reflect.TypeFor[localEmissionDestinationValue]()
	foreignKey, foreignValue := reflect.TypeFor[localEmissionForeignKey](), reflect.TypeFor[localEmissionOtherValue]()
	baseValue := reflect.TypeFor[localEmissionValue]()
	types := map[string]reflect.Type{
		base + ".Key": reflect.TypeFor[localEmissionBaseKey](), base + ".Value": baseValue,
		destination + ".Key": keyType, destination + ".Value": valueType,
		third + ".Key": foreignKey, third + ".Value": foreignValue,
		fourth + ".Value": reflect.TypeFor[localEmissionFourthValue](),
	}
	lookup := testTypeResolver(func(name string) (*x.Type, error) {
		name = unwrapQualifiedTypeName(name)
		// Deliberately wrong bare-name/default-package candidates have the same
		// names and nonempty fields as the destination's genuine types.
		if name == "Key" {
			name = base + ".Key"
		}
		if name == "Value" {
			name = base + ".Value"
		}
		for _, imp := range imports {
			if strings.HasPrefix(name, imp.Alias+".") {
				name = imp.Package + strings.TrimPrefix(name, imp.Alias)
			}
		}
		typ := types[name]
		if typ == nil {
			return nil, fmt.Errorf("unexpected canonical key %s", name)
		}
		index := strings.LastIndex(name, ".")
		return &x.Type{Name: name[index+1:], PkgPath: name[:index], Type: typ}, nil
	})
	resolver := fieldTypeResolver{plan: plan, targetPackage: base, context: &spec.TypeContext{DefaultPackage: base, Imports: imports}, lookup: lookup.Descriptor}
	wantFields := map[string]string{}
	wantRuntime := map[string]reflect.Type{}
	for _, tc := range []struct {
		name, authored, emitted string
		runtime                 reflect.Type
	}{
		{"Local", "map[dto.Key][]*dto.Value", "map[Key][]*Value", reflect.MapOf(keyType, reflect.SliceOf(reflect.PointerTo(valueType)))},
		{"Mixed", "map[dto.Key]*foreign.Value", "map[Key]*foreign.Value", reflect.MapOf(keyType, reflect.PointerTo(foreignValue))},
		{"Reverse", "map[foreign.Key]*dto.Value", "map[foreign.Key]*Value", reflect.MapOf(foreignKey, reflect.PointerTo(valueType))},
		{"Nested", "map[dto.Key]map[foreign.Key]*[]*dto.Value", "map[Key]map[foreign.Key]*[]*Value", reflect.MapOf(keyType, reflect.MapOf(foreignKey, reflect.PointerTo(reflect.SliceOf(reflect.PointerTo(valueType)))))},
		{"Base", "[]*self.Value", "[]*self.Value", reflect.SliceOf(reflect.PointerTo(baseValue))},
	} {
		actual, err := resolver.resolve(tc.authored)
		if err != nil {
			t.Fatal(err)
		}
		plan.Input.Fields = append(plan.Input.Fields, Field{Name: tc.name, Type: actual})
		plan.Output.Fields = append(plan.Output.Fields, Field{Name: tc.name, Type: actual})
		// The expected expression and nominal type are checked after partition.
		wantFields[tc.name] = tc.emitted
		wantRuntime[tc.name] = tc.runtime
	}
	// A full canonical external path must retain an allocated alias when its
	// basename is already reserved by a different imported package.
	if actual, err := resolver.resolve("*" + fourth + ".Value"); err != nil || actual != "*model2.Value" {
		t.Fatalf("colliding external descriptor alias=%s, %v", actual, err)
	}
	collisionMap, err := emittedMapColumnType(plan, "map["+destination+".Key]*"+fourth+".Value")
	if err != nil {
		t.Fatal(err)
	}
	plan.Input.Fields = append(plan.Input.Fields, Field{Name: "Collision", Type: collisionMap})
	plan.Output.Fields = append(plan.Output.Fields, Field{Name: "Collision", Type: collisionMap})
	wantFields["Collision"] = "map[Key]*model2.Value"
	wantRuntime["Collision"] = reflect.MapOf(keyType, reflect.PointerTo(reflect.TypeFor[localEmissionFourthValue]()))
	if err := destinations.partition(plan); err != nil {
		t.Fatal(err)
	}
	if len(plan.ShapePackages) != 1 {
		t.Fatalf("destination groups=%d", len(plan.ShapePackages))
	}
	group := plan.ShapePackages[0]
	wantAuthority := map[string]string{"Key": destination + ".Key", "Value": destination + ".Value"}
	if !reflect.DeepEqual(group.knownLocalTypes, wantAuthority) {
		t.Fatalf("wrong destination authority: %+v", group.knownLocalTypes)
	}
	for _, contract := range []ContractPlan{group.Input, group.Output} {
		for _, field := range contract.Fields {
			if field.Type != wantFields[field.Name] {
				t.Fatalf("%s expression=%s; want %s", field.Name, field.Type, wantFields[field.Name])
			}
		}
	}
	for _, materialize := range []func() (reflect.Type, error){newRuntimeInputMaterializer(group, lookup).inputType, newRuntimeInputMaterializer(group, lookup).outputType} {
		actual, err := materialize()
		if err != nil {
			t.Fatal(err)
		}
		for name, want := range wantRuntime {
			field, ok := actual.FieldByName(name)
			if !ok || field.Type != want {
				t.Fatalf("destination runtime %s = %+v; want %v", name, field, want)
			}
		}
	}
	for _, failure := range []struct {
		name       string
		descriptor func() (*x.Type, error)
	}{
		{"absent", func() (*x.Type, error) { return nil, nil }},
		{"nil compiled type", func() (*x.Type, error) { return &x.Type{Name: "Value", PkgPath: destination}, nil }},
		{"error", func() (*x.Type, error) { return nil, errors.New("destination descriptor unavailable") }},
	} {
		t.Run(failure.name, func(t *testing.T) {
			unavailable := testTypeResolver(func(name string) (*x.Type, error) {
				if name == destination+".Value" {
					return failure.descriptor()
				}
				return lookup.Descriptor(name)
			})
			for _, materialize := range []func() (reflect.Type, error){newRuntimeInputMaterializer(group, unavailable).inputType, newRuntimeInputMaterializer(group, unavailable).outputType} {
				if actual, err := materialize(); err == nil {
					t.Fatalf("missing canonical destination descriptor produced %v", actual)
				}
			}
		})
	}
	files, err := EmitScaffold(filepath.Join(root, "destination/model"), group)
	if err != nil {
		t.Fatal(err)
	}
	localEmissionAssertSources(t, files)
	localEmissionWrite(t, root, "destination/model/identity_test.go", `package model
import ("testing"; self "example.com/localtypes/model"; foreign "example.com/localtypes/third/model"; model2 "example.com/localtypes/fourth/model")
func TestRelocatedNominalIdentity(t *testing.T) {
 var _ map[Key][]*Value = Request{}.Local
 var _ map[Key][]*Value = Response{}.Local
 var _ map[Key]*foreign.Value = Request{}.Mixed
 var _ map[Key]*foreign.Value = Response{}.Mixed
 var _ map[foreign.Key]*Value = Request{}.Reverse
 var _ map[foreign.Key]*Value = Response{}.Reverse
 var _ map[Key]map[foreign.Key]*[]*Value = Request{}.Nested
 var _ map[Key]map[foreign.Key]*[]*Value = Response{}.Nested
 var _ []*self.Value = Request{}.Base
 var _ []*self.Value = Response{}.Base
 var _ map[Key]*model2.Value = Request{}.Collision
 var _ map[Key]*model2.Value = Response{}.Collision
}
`)
	localEmissionCompile(t, root)
	for _, authority := range []struct{ relative, bytes string }{{"model/authority.go", baseAuthority}, {"destination/model/authority.go", destinationAuthority}, {"third/model/authority.go", thirdAuthority}, {"fourth/model/authority.go", fourthAuthority}} {
		actual, err := os.ReadFile(filepath.Join(root, authority.relative))
		if err != nil || string(actual) != authority.bytes {
			t.Fatalf("authored authority modified: %s: %v", authority.relative, err)
		}
	}
	if count := localEmissionDeclarationCount(t, filepath.Join(root, "destination/model"), "Value"); count != 1 {
		t.Fatalf("destination Value declarations=%d", count)
	}
	if _, err := EmitScaffold(filepath.Join(root, "model"), plan); err == nil || !strings.Contains(err.Error(), "generation import cycle") {
		t.Fatalf("genuine reverse dependency cycle accepted: %v", err)
	}
}

func TestLocalTypeEmissionRelocationRequiresExactProvenance(t *testing.T) {
	const base = "example.com/localtypes/model"
	const destination = "example.com/localtypes/dto"
	const different = "example.com/localtypes/third/model"
	for _, tc := range []struct {
		name, expression string
		descriptor       *x.Type
		expectedProof    string
	}{
		{"absent proof", "dto.Value", nil, ""},
		{"empty package", "dto.Value", &x.Type{Name: "Value"}, ""},
		{"same name different package", "dto.Value", &x.Type{Name: "Value", PkgPath: different}, different + ".Value"},
		{"arbitrary alias", "arbitrary.Value", nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := localEmissionModule(t)
			imports := []spec.ImportSpec{{Alias: "dto", Package: destination}}
			plan, destinations := localEmissionRelocationSetup(t, root, base, destination, imports)
			expression := tc.expression
			if tc.descriptor != nil {
				resolver := fieldTypeResolver{plan: plan, targetPackage: base, context: &spec.TypeContext{Imports: imports}, lookup: func(string) (*x.Type, error) { return tc.descriptor, nil }}
				var err error
				expression, err = resolver.resolve(expression)
				if err != nil {
					t.Fatal(err)
				}
			}
			plan.Input.Fields = []Field{{Name: "Value", Type: expression}}
			plan.Output.Fields = []Field{{Name: "Value", Type: expression}}
			if tc.expectedProof != "" && !plan.resolvedNamedTypes[tc.expectedProof] {
				t.Fatalf("actual external descriptor proof lost: %+v", plan.resolvedNamedTypes)
			}
			if plan.resolvedNamedTypes[destination+".Value"] {
				t.Fatal("destination identity fabricated before relocation")
			}
			if err := destinations.partition(plan); err != nil {
				t.Fatal(err)
			}
			group := plan.ShapePackages[0]
			if len(group.knownLocalTypes) != 0 {
				t.Fatalf("unproven destination identity promoted: %+v", group.knownLocalTypes)
			}
			if tc.name == "arbitrary alias" {
				if group.Input.Fields[0].Type != "arbitrary.Value" {
					t.Fatalf("unknown alias changed: %+v", group.Input.Fields)
				}
			} else if actual := group.referencedPlaceholderTypes(); !reflect.DeepEqual(actual, []string{"Value"}) {
				t.Fatalf("unknown placeholder compatibility changed: %v", actual)
			}
		})
	}
}

func localEmissionRelocationSetup(t *testing.T, root, base, destination string, imports []spec.ImportSpec) (*Plan, *shapeDestinations) {
	t.Helper()
	component := &spec.Component{Settings: &spec.Settings{InputType: "dto.Request", OutputType: "dto.Response"}, TypeContext: &spec.TypeContext{DefaultPackage: base, Imports: imports}, RootView: &spec.View{Name: "Records", TypeName: "dto.Record", Dest: "record.go"}}
	destinations := &shapeDestinations{input: &Input{Component: component, TargetPackage: base, ProjectRoot: root}}
	if err := destinations.prepare(); err != nil {
		t.Fatal(err)
	}
	plan := &Plan{Package: base, GoPackage: "model", ProjectRoot: root, ComponentName: "Relocation", Generation: &spec.GenerationSettings{}, ShapesOnly: true, RouterDest: "router.go", ViewDest: "record.go", RootViewType: "Record", Imports: append([]spec.ImportSpec(nil), imports...), Input: ContractPlan{Type: "Request", Destination: "request.go", Ownership: ContractGenerated}, Output: ContractPlan{Type: "Response", Destination: "response.go", Ownership: ContractGenerated}, Views: []ViewPlan{{Name: "Record", Type: "Record", Identity: RootViewPath, Ownership: ViewGenerated, Destination: "record.go"}}}
	return plan, destinations
}

func localEmissionDeclarationCount(t *testing.T, directory, name string) int {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), filepath.Join(directory, entry.Name()), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range parsed.Decls {
			general, ok := declaration.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, item := range general.Specs {
				typ, ok := item.(*ast.TypeSpec)
				if ok && typ.Name.Name == name {
					count++
				}
			}
		}
	}
	return count
}

func TestLocalTypeEmissionKnownDescriptorDoesNotCreatePlaceholder(t *testing.T) {
	const local = "example.com/localtypes/model"
	type value struct{ Label string }
	catalog := typecatalog.NewCatalog()
	if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeOf(value{}), x.WithName("Value"), x.WithPkgPath(local))); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{PackagePath: local, Imports: []typecatalog.PackageImport{{Alias: "self", Package: local}}})
	if err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{Name: "Records", TypeContext: &spec.TypeContext{DefaultPackage: local, Imports: []spec.ImportSpec{{Alias: "self", Package: local}}}, Parameters: []*spec.Parameter{{Name: "Values", TypeExpr: "[]*self.Value", Source: spec.BindSource{Kind: "transient"}}}}
	plan, err := New(Input{Component: component, TargetPackage: local, TypeResolver: resolver}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Input.Fields) != 1 || plan.Input.Fields[0].Type != "[]*Value" {
		t.Fatalf("known local type not normalized: %+v", plan.Input.Fields)
	}
	for _, placeholder := range plan.referencedPlaceholderTypes() {
		if placeholder == "Value" {
			t.Fatal("resolved same-package descriptor became a duplicate generated placeholder")
		}
	}
}

type localEmissionValue struct{ Label string }
type localEmissionOtherValue struct{ Number int }

func TestLocalTypeEmissionRuntimeCanonicalIdentity(t *testing.T) {
	const local = "example.com/localtypes/model"
	const external = "example.com/localtypes/elsewhere/model"
	localType, externalType := reflect.TypeFor[localEmissionValue](), reflect.TypeFor[localEmissionOtherValue]()
	for _, tc := range []struct {
		expression string
		want       reflect.Type
	}{
		{"self.Value", localType},
		{"*self.Value", reflect.PointerTo(localType)},
		{"[]*self.Value", reflect.SliceOf(reflect.PointerTo(localType))},
		{"*[]self.Value", reflect.PointerTo(reflect.SliceOf(localType))},
		{"map[string][]*self.Value", reflect.MapOf(reflect.TypeFor[string](), reflect.SliceOf(reflect.PointerTo(localType)))},
		{"map[string]map[string]*[]*self.Value", reflect.MapOf(reflect.TypeFor[string](), reflect.MapOf(reflect.TypeFor[string](), reflect.PointerTo(reflect.SliceOf(reflect.PointerTo(localType)))))},
		{"*other.Value", reflect.PointerTo(externalType)},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			component := &spec.Component{Name: "Identity", TypeContext: &spec.TypeContext{DefaultPackage: external, Imports: []spec.ImportSpec{{Alias: "self", Package: local}, {Alias: "other", Package: external}}}, Parameters: []*spec.Parameter{
				{Name: "InputValue", TypeExpr: tc.expression, Source: spec.BindSource{Kind: "transient"}},
				{Name: "OutputValue", TypeExpr: tc.expression, Source: spec.BindSource{Kind: "output", Name: "body"}},
			}}
			generator := New(Input{Component: component, TargetPackage: local})
			generator.resolver = testTypeResolver(func(name string) (*x.Type, error) {
				if name == "self.Value" || name == local+".Value" {
					return &x.Type{Name: "Value", PkgPath: local, Type: localType}, nil
				}
				// A bare-name/default-package lookup is a deliberate wrong-type trap.
				if name == "Value" || name == "other.Value" || name == external+".Value" {
					return &x.Type{Name: "Value", PkgPath: external, Type: externalType}, nil
				}
				return nil, fmt.Errorf("unexpected type authority lookup %s", name)
			})
			for _, method := range []struct {
				name, field string
				materialize func() (reflect.Type, error)
			}{{"input", "InputValue", generator.RuntimeInputType}, {"output", "OutputValue", generator.RuntimeOutputType}} {
				actual, err := method.materialize()
				if err != nil {
					t.Fatalf("%s: %v", method.name, err)
				}
				field, ok := actual.FieldByName(method.field)
				if !ok || field.Type != tc.want {
					t.Fatalf("%s nominal field=%+v; want %v", method.name, field, tc.want)
				}
			}
		})
	}
}

func TestLocalTypeEmissionRuntimeFailsClosed(t *testing.T) {
	const local = "example.com/localtypes/model"
	component := &spec.Component{Name: "Unavailable", TypeContext: &spec.TypeContext{DefaultPackage: local, Imports: []spec.ImportSpec{{Alias: "self", Package: local}}}, Parameters: []*spec.Parameter{
		{Name: "InputValue", TypeExpr: "*self.Value", Source: spec.BindSource{Kind: "transient"}},
		{Name: "OutputValue", TypeExpr: "*self.Value", Source: spec.BindSource{Kind: "output", Name: "body"}},
	}}
	for _, tc := range []struct {
		name, want string
		runtime    func() (*x.Type, error)
	}{
		{"error", "canonical lookup failed", func() (*x.Type, error) { return nil, errors.New("canonical lookup failed") }},
		{"absent", "not found", func() (*x.Type, error) { return nil, nil }},
		{"nil runtime", "no compiled runtime type", func() (*x.Type, error) { return &x.Type{Name: "Value", PkgPath: local}, nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			generator := New(Input{Component: component, TargetPackage: local})
			generator.resolver = testTypeResolver(func(name string) (*x.Type, error) {
				if name == "self.Value" {
					return &x.Type{Name: "Value", PkgPath: local, Type: reflect.TypeFor[localEmissionValue]()}, nil
				}
				if name != local+".Value" {
					t.Fatalf("runtime lost canonical key: %s", name)
				}
				return tc.runtime()
			})
			for _, materialize := range []func() (reflect.Type, error){generator.RuntimeInputType, generator.RuntimeOutputType} {
				if actual, err := materialize(); err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("unavailable authority produced %v, %v; want %s", actual, err, tc.want)
				}
			}
		})
	}
	plan := &Plan{Input: ContractPlan{Type: "Input", Ownership: ContractGenerated, Fields: []Field{{Name: "Value", Type: "*Value"}}}}
	resolver := fieldTypeResolver{plan: plan, targetPackage: local}
	resolver.emittedNamedType("self.Value", &x.Type{Name: "Value", PkgPath: local, Type: reflect.TypeFor[localEmissionValue]()})
	if _, err := newRuntimeInputMaterializer(plan, nil).inputType(); err == nil || !strings.Contains(err.Error(), "requires type authority") {
		t.Fatalf("missing runtime resolver accepted: %v", err)
	}
}

func TestLocalTypeEmissionPlaceholderScopeAndPrecedence(t *testing.T) {
	const local = "example.com/localtypes/model"
	plan := &Plan{Input: ContractPlan{Type: "Input", Ownership: ContractGenerated, Fields: []Field{
		{Name: "Known", Type: "*Value", Tag: `typeName:"Value"`},
		{Name: "Unknown", Type: "*Unknown", Tag: `typeName:"UnknownTag"`},
	}}}
	resolver := fieldTypeResolver{plan: plan, targetPackage: local}
	resolver.emittedNamedType("self.Value", &x.Type{Name: "Value", PkgPath: local})
	if actual := plan.referencedPlaceholderTypes(); !reflect.DeepEqual(actual, []string{"Unknown", "UnknownTag"}) {
		t.Fatalf("placeholder policy changed: %v", actual)
	}
	other := &Plan{Input: plan.Input}
	if shouldSkipPlaceholderType(other, "Value") {
		t.Fatal("local authority leaked into another plan")
	}
	resolver.plan = nil
	if actual := resolver.emittedNamedType("self.Value", &x.Type{Name: "Value", PkgPath: local}); actual != "Value" {
		t.Fatalf("nil-plan resolver changed: %s", actual)
	}
	unknown := New(Input{Component: &spec.Component{Name: "Unknown", Parameters: []*spec.Parameter{{Name: "Body", TypeExpr: "*Unknown", Source: spec.BindSource{Kind: "body"}}}}})
	actual, err := unknown.RuntimeInputType()
	if err != nil {
		t.Fatal(err)
	}
	field, ok := actual.FieldByName("Body")
	if !ok || field.Type.Kind() != reflect.Pointer || field.Type.Elem().NumField() != 0 {
		t.Fatalf("unknown scaffold no longer supported: %+v", field)
	}
	for _, helper := range []bool{false, true} {
		generated := &Plan{Input: ContractPlan{Type: "Input", Ownership: ContractGenerated, Fields: []Field{{Name: "Value", Type: "*Value"}}}}
		resolver.plan = generated
		resolver.emittedNamedType("self.Value", &x.Type{Name: "Value", PkgPath: local})
		fields := []Field{{Name: "Generated", Type: "string"}}
		if helper {
			generated.HelperTypes = []HelperType{{Name: "Value", Fields: fields}}
		} else {
			generated.Views = []ViewPlan{{Name: "Value", Type: "Value", Ownership: ViewGenerated, Fields: fields}}
		}
		actual, err := newRuntimeInputMaterializer(generated, testTypeResolver(func(string) (*x.Type, error) { t.Fatal("generated type lost precedence"); return nil, nil })).inputType()
		if err != nil {
			t.Fatal(err)
		}
		value, _ := actual.FieldByName("Value")
		if _, ok := value.Type.Elem().FieldByName("Generated"); !ok {
			t.Fatalf("generated shape erased: %+v", value)
		}
	}
}

func TestLocalTypeEmissionCompleteKnownDeclarationFixture(t *testing.T) {
	const local = "example.com/localtypes/model"
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/localtypes"}).Write(t, root)
	authority := "package model\ntype Value struct { Label string }\n"
	localEmissionWrite(t, root, "model/authority.go", authority)
	component, err := parseTestComponentSource(local, "Records", `#package('example.com/localtypes/model')
#import('self','example.com/localtypes/model')
#setting($_ = $route('/records','GET'))
#define($_ = $Values<[]*self.Value>(transient/).WithTag('typeName:"Value"'))
#define($_ = $Result<*self.Value>(output/body))
SELECT id FROM records`)
	if err != nil {
		t.Fatal(err)
	}
	generator := New(Input{Component: component, TargetPackage: local, PackageName: "model", ProjectRoot: root})
	generator.resolver = testTypeResolver(func(name string) (*x.Type, error) {
		if name == "self.Value" || name == local+".Value" {
			return &x.Type{Name: "Value", PkgPath: local, Type: reflect.TypeFor[localEmissionValue]()}, nil
		}
		return nil, fmt.Errorf("unexpected lookup %s", name)
	})
	result, err := generator.Generate(filepath.Join(root, "model"))
	if err != nil {
		t.Fatal(err)
	}
	localEmissionAssertSources(t, result.Files)
	if field, _ := result.Plan.Input.Field("Values"); field.Type != "[]*Value" {
		t.Fatalf("input reference changed: %+v", field)
	}
	if field, _ := result.Plan.Output.Field("Result"); field.Type != "*Value" {
		t.Fatalf("output reference changed: %+v", field)
	}
	before, err := os.ReadFile(filepath.Join(root, "model/authority.go"))
	if err != nil || string(before) != authority {
		t.Fatalf("authority declaration modified: %s, %v", before, err)
	}
	declarations := 0
	entries, err := os.ReadDir(filepath.Join(root, "model"))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, "model", entry.Name()), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range parsed.Decls {
			general, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, item := range general.Specs {
				typ, ok := item.(*ast.TypeSpec)
				if ok && typ.Name.Name == "Value" {
					declarations++
				}
			}
		}
	}
	if declarations != 1 {
		t.Fatalf("Value declarations=%d; want existing declaration only", declarations)
	}
	localEmissionWrite(t, root, "model/identity_test.go", `package model
import "testing"
func TestExistingDeclaration(t *testing.T) {
 var _ []*Value = RecordsInput{}.Values
 var _ *Value = RecordsOutput{}.Result
 if (Value{Label:"kept"}).Label != "kept" { t.Fatal("field erased") }
}
`)
	localEmissionCompile(t, root)
	// Known descriptor metadata must not hide independent duplicate declarations.
	localEmissionWrite(t, root, "model/duplicate.go", "package model\ntype Value struct { Other int }\n")
	if _, err := generator.Generate(filepath.Join(root, "model")); err == nil || !strings.Contains(err.Error(), "declaration Value collides") {
		t.Fatalf("independent duplicate declaration accepted: %v", err)
	}
}

func TestLocalTypeEmissionPreservesIndependentAuthoredCycle(t *testing.T) {
	root := localEmissionModule(t)
	localEmissionWrite(t, root, "one/one.go", "package one\nimport two \"example.com/localtypes/two\"\ntype One struct { Next *two.Two }\n")
	localEmissionWrite(t, root, "two/two.go", "package two\nimport one \"example.com/localtypes/one\"\ntype Two struct { Next *one.One }\n")
	set := &packageSet{plans: []*Plan{{ProjectRoot: root, Package: "example.com/localtypes/generated", GoPackage: "generated"}}, dirs: []string{filepath.Join(root, "generated")}, files: [][]EmittedFile{{{Path: filepath.Join(root, "generated/record.go"), Content: "package generated\ntype Record struct{}\n"}}}}
	if err := set.validateImports(); err == nil || !strings.Contains(err.Error(), "generation import cycle") {
		t.Fatalf("independent authored cycle accepted: %v", err)
	}
}

func localEmissionModule(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	localEmissionWrite(t, root, "go.mod", "module example.com/localtypes\n\ngo 1.25.8\n")
	return root
}

func localEmissionWrite(t *testing.T, root, relative, content string) {
	t.Helper()
	path := filepath.Join(root, relative)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func localEmissionAssertSources(t *testing.T, files []EmittedFile) {
	t.Helper()
	for _, file := range files {
		parsed, err := parser.ParseFile(token.NewFileSet(), file.Path, file.Content, parser.AllErrors)
		if err != nil {
			t.Fatalf("parse %s: %v", file.Path, err)
		}
		root, ok := findModuleRoot(filepath.Dir(file.Path))
		if !ok {
			t.Fatalf("generated fixture module not found: %s", file.Path)
		}
		module, err := readModulePath(root)
		if err != nil {
			t.Fatal(err)
		}
		relative, err := filepath.Rel(root, filepath.Dir(file.Path))
		if err != nil {
			t.Fatal(err)
		}
		self := module + "/" + filepath.ToSlash(relative)
		for _, imported := range parsed.Imports {
			packagePath := strings.Trim(imported.Path.Value, "\"")
			if packagePath == self {
				t.Fatalf("generated self import in %s: %s", file.Path, packagePath)
			}
		}
	}
}

func localEmissionCompile(t *testing.T, root string) {
	t.Helper()
	// This fixture consists solely of emitted Go shapes and real nominal types;
	// it needs no SDK dependency resolution or module graph modification.
	command := exec.Command("go", "test", "-race", "-count=1", "-mod=readonly", "./...")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on", "GOMAXPROCS=2")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated nominal fixture compile: %v\n%s", err, output)
	}
}
