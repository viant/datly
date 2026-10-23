package transcribe

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
	x "github.com/viant/x"
)

func artifactFixture(t testing.TB) *RuntimeContract {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(`CREATE TABLE records (ID INTEGER PRIMARY KEY, NAME TEXT); INSERT INTO records VALUES (1, NULL), (2, 'two')`); err != nil {
		t.Fatal(err)
	}
	contract, err := NewCompiler().RuntimeContractsInModule(context.Background(), "example.com/app", &Source{
		Name: "Records", Scope: "example.com/app/readers/records", Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db}), Text: `#package('example.com/app/readers/records')
#setting($_ = $connector('main'))
#setting($_ = $route('/records','GET'))
#define($_ = $Rows<[]*Row>(output/view))
SELECT records.*, type(records,'Row') FROM (SELECT * FROM records) records`,
	})
	if err != nil {
		t.Fatal(err)
	}
	return contract
}

func TestRuntimeContractArtifactResolvedReaderRoundtrip(t *testing.T) {
	contract := artifactFixture(t)
	// Schema discovery's connection has been closed by the fixture.
	frozen, err := contract.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	before, err := frozen.Hash()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(frozen)
	if err != nil {
		t.Fatal(err)
	}
	var stored RuntimeContractArtifact
	if err = json.Unmarshal(raw, &stored); err != nil {
		t.Fatal(err)
	}
	after, err := stored.Hash()
	if err != nil || after != before {
		t.Fatalf("hash changed: %s %s %v", before, after, err)
	}
	restored, err := stored.Materialize(nil, contract.Resources)
	if err != nil {
		t.Fatal(err)
	}
	if restored.InputType != contract.InputType || restored.OutputType != contract.OutputType {
		t.Fatalf("runtime shapes changed: %v %v", restored.InputType, restored.OutputType)
	}
	if restored.Plan != nil || restored.Result != nil {
		t.Fatal("hydration retained authoring products")
	}
	originalJSON, _ := json.Marshal(contract.Component)
	restoredJSON, _ := json.Marshal(restored.Component)
	if string(originalJSON) != string(restoredJSON) {
		t.Fatalf("component metadata changed: %s\n%s", originalJSON, restoredJSON)
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: restored.Component, InputType: restored.InputType, OutputType: restored.OutputType, Types: restored.Types, Resources: restored.Resources})
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(`CREATE TABLE records (ID INTEGER PRIMARY KEY, NAME TEXT); INSERT INTO records VALUES (1, NULL), (2, 'two')`); err != nil {
		t.Fatal(err)
	}
	execution, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db}})
	if err != nil {
		t.Fatal(err)
	}
	actual, err := execution.Read(context.Background(), reflect.New(restored.InputType).Interface(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(actual)
	if err != nil || !strings.Contains(string(encoded), "two") {
		t.Fatalf("reader output: %s %v", encoded, err)
	}
	restored.Component.Name = "mutated"
	if stored.Component.Name == "mutated" || contract.Component.Name == "mutated" {
		t.Fatal("component generations share mutable state")
	}
}

type artifactLinkedValue struct{ Value string }

func (artifactLinkedValue) String() string { return "linked" }

func TestRuntimeContractArtifactShapesAndNamedIdentity(t *testing.T) {
	catalog := typecatalog.NewCatalog()
	named := reflect.TypeFor[artifactLinkedValue]()
	if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(named)); err != nil {
		t.Fatal(err)
	}
	shape := reflect.TypeFor[struct {
		Value   artifactLinkedValue  `json:"value"`
		When    *time.Time           `json:"when,omitempty"`
		Items   map[string][]*[2]int `json:"items"`
		Payload any                  `json:"payload"`
	}]()
	contract := &RuntimeContract{Component: &spec.Component{Name: "shapes"}, InputType: shape, OutputType: shape, Types: catalog}
	frozen, err := contract.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	hydrated, err := frozen.Materialize(catalog, nil)
	if err != nil {
		t.Fatal(err)
	}
	if hydrated.InputType != shape {
		t.Fatalf("shape changed: %v", hydrated.InputType)
	}
	f, _ := hydrated.InputType.FieldByName("Value")
	if f.Type != named || f.Type.NumMethod() != 1 {
		t.Fatal("named method identity lost")
	}
	if _, err = frozen.Materialize(nil, nil); err == nil {
		t.Fatal("missing compiled named type accepted")
	}
	for i := range frozen.Types {
		if frozen.Types[i].Kind == "named" && frozen.Types[i].Name == "artifactLinkedValue" {
			frozen.Types[i].Fingerprint = "wrong"
		}
	}
	if _, err = frozen.Materialize(catalog, nil); err == nil {
		t.Fatal("incompatible named identity accepted")
	}
}

func TestRuntimeContractArtifactRejectsInvalidArtifacts(t *testing.T) {
	base := &RuntimeContract{Component: &spec.Component{}, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[struct{}]()}
	for _, tc := range []struct {
		name   string
		change func(*RuntimeContractArtifact)
	}{
		{"version", func(a *RuntimeContractArtifact) { a.Version = "future" }},
		{"duplicate", func(a *RuntimeContractArtifact) { a.Types = append(a.Types, a.Types[0]) }},
		{"missing", func(a *RuntimeContractArtifact) { a.Input = "missing" }},
		{"cycle", func(a *RuntimeContractArtifact) {
			a.Types[0] = RuntimeArtifactType{ID: "t0", Kind: "slice", Element: "t0"}
		}},
		{"bad field", func(a *RuntimeContractArtifact) {
			a.Types[0] = RuntimeArtifactType{ID: "t0", Kind: "struct", Fields: []RuntimeArtifactField{{Name: "bad", Type: "t1"}}}
			a.Types = append(a.Types, RuntimeArtifactType{ID: "t1", Kind: "builtin", Name: "int"})
		}},
		{"resources", func(a *RuntimeContractArtifact) { a.ResourcesRequired = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, err := base.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			tc.change(a)
			if _, err = a.Materialize(nil, nil); err == nil {
				t.Fatal("invalid artifact accepted")
			}
		})
	}
	for _, typ := range []reflect.Type{reflect.TypeFor[func()](), reflect.TypeFor[chan int](), reflect.TypeFor[interface{ String() string }](), reflect.TypeFor[struct{ hidden int }]()} {
		base.InputType = typ
		if _, err := base.Freeze(); err == nil {
			t.Fatalf("unsupported structural shape accepted: %v", typ)
		}
	}
}

func BenchmarkRuntimeContractArtifactMaterialize(b *testing.B) {
	contract := artifactFixture(b)
	artifact, err := contract.Freeze()
	if err != nil {
		b.Fatal(err)
	}
	raw, err := json.Marshal(artifact)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var loaded RuntimeContractArtifact
		if err = json.Unmarshal(raw, &loaded); err != nil {
			b.Fatal(err)
		}
		if _, err = loaded.Materialize(nil, contract.Resources); err != nil {
			b.Fatal(err)
		}
	}
}

func TestRuntimeContractArtifactCatalogOwnership(t *testing.T) {
	catalog := typecatalog.NewCatalog()
	generated := reflect.TypeFor[struct {
		ID int `json:"id"`
	}]()
	authored := reflect.TypeFor[struct {
		Code string `json:"code"`
	}]()
	for _, entry := range []struct {
		origin typecatalog.TypeOrigin
		typ    reflect.Type
	}{
		{typecatalog.TypeOriginGenerated, generated}, {typecatalog.TypeOriginDQL, authored},
	} {
		if err := catalog.Register(entry.origin, x.NewType(entry.typ, x.WithName("Record"), x.WithPkgPath("example.com/app"))); err != nil {
			t.Fatal(err)
		}
	}
	c := &RuntimeContract{Component: &spec.Component{}, InputType: generated, OutputType: authored, Types: catalog}
	frozen, err := c.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	hydrated, err := frozen.Materialize(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, authority := range []typecatalog.Authority{typecatalog.PackageAuthority, typecatalog.TranscribeAuthority} {
		before, _, err := catalog.ResolveRuntimeType(authority, "example.com/app.Record")
		if err != nil {
			t.Fatal(err)
		}
		after, _, err := hydrated.Types.ResolveRuntimeType(authority, "example.com/app.Record")
		if err != nil || before != after {
			t.Fatalf("%s identity changed: %v %v %v", authority, before, after, err)
		}
		beforeOrigin, _, _ := catalog.Origin(authority, "example.com/app.Record")
		afterOrigin, _, _ := hydrated.Types.Origin(authority, "example.com/app.Record")
		if beforeOrigin != afterOrigin {
			t.Fatalf("%s origin changed", authority)
		}
	}
}

func TestRuntimeContractArtifactMetadataAndUnsupportedAuthority(t *testing.T) {
	contract := artifactFixture(t)
	contract.Component.Settings.Report = &spec.ReportSettings{Enabled: true}
	contract.Component.RootView.Selector = &spec.Selector{AllowLimit: true, DefaultLimit: 500, Specified: []spec.SelectorProperty{spec.SelectorPropertyLimit}}
	contract.Component.RootView.Relations = []*spec.Relation{{Name: "Children", Kind: spec.RelationKind("subview"), Cardinality: spec.CardinalityMany,
		On: []*spec.RelationLink{{ParentField: "ID", ChildField: "ParentID"}}, View: &spec.View{Name: "child", Columns: []*spec.Column{{Name: "ParentID", Type: spec.TypeRef{Name: "int"}}}}}}
	frozen, err := contract.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	hydrated, err := frozen.Materialize(nil, contract.Resources)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(contract.Component)
	after, _ := json.Marshal(hydrated.Component)
	if string(before) != string(after) {
		t.Fatalf("relation/selector/report metadata changed: %s\n%s", before, after)
	}
	contract.Component.Settings.Mutation = "patch"
	if _, err = contract.Freeze(); err == nil {
		t.Fatal("writer accepted")
	}
	contract.Component.Settings.Mutation = ""
	contract.Types = typecatalog.NewCatalog()
	if err = contract.Types.Register(typecatalog.TypeOriginDQL, &x.Type{Name: "SourceOnly", PkgPath: "example.com/app", Definition: "struct { ID int }"}); err != nil {
		t.Fatal(err)
	}
	if _, err = contract.Freeze(); err == nil || !strings.Contains(err.Error(), "source descriptor") {
		t.Fatalf("source-only authority accepted: %v", err)
	}
}

func TestRuntimeContractArtifactRejectsSeedOwnershipChange(t *testing.T) {
	typ := reflect.TypeFor[struct{ ID int }]()
	source := typecatalog.NewCatalog()
	if err := source.Register(typecatalog.TypeOriginGenerated, x.NewType(typ, x.WithPkgPath("example.com/app"), x.WithName("Row"))); err != nil {
		t.Fatal(err)
	}
	contract := &RuntimeContract{Component: &spec.Component{}, InputType: typ, OutputType: typ, Types: source}
	artifact, err := contract.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	seed := typecatalog.NewCatalog()
	if err = seed.Register(typecatalog.TypeOriginPackage, x.NewType(typ, x.WithPkgPath("example.com/app"), x.WithName("Row"))); err != nil {
		t.Fatal(err)
	}
	if _, err = artifact.Materialize(seed, nil); err == nil {
		t.Fatal("seed changed published catalog ownership")
	}
}

func TestRuntimeContractArtifactLinkedCatalogAlias(t *testing.T) {
	catalog := typecatalog.NewCatalog()
	typ := reflect.TypeFor[artifactLinkedValue]()
	if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(typ, x.WithPkgPath("example.com/alias"), x.WithName("Value"))); err != nil {
		t.Fatal(err)
	}
	contract := &RuntimeContract{Component: &spec.Component{}, InputType: typ, OutputType: typ, Types: catalog}
	artifact, err := contract.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := artifact.Materialize(catalog, nil)
	if err != nil {
		t.Fatal(err)
	}
	if restored.InputType != typ {
		t.Fatal("linked alias lost compiled identity")
	}
}
