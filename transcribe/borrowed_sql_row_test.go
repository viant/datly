package transcribe

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/constant"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
)

func borrowedAuxiliaryNullComparisonFixture(t *testing.T) (*Result, *spec.View, spec.BorrowedSQLRow) {
	t.Helper()
	ctx := context.Background()
	db := sqlite.New(t, sqlite.WithDSN(filepath.Join(t.TempDir(), "exclusive-nested-null-comparison.db")))
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER PRIMARY KEY)", "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,parent_id INTEGER REFERENCES parents(id),name TEXT)"))
	refiner := column.New(column.Connections{"main": db.DB})
	leaf := &spec.View{Name: "Rows", TypeName: "Row", Cardinality: spec.CardinalityMany, Source: &spec.ViewSource{Table: "records", SQL: "SELECT r.id,r.parent_id,r.name FROM records r"}}
	root := &spec.View{Name: "Parents", TypeName: "Parent", Cardinality: spec.CardinalityMany, Source: &spec.ViewSource{Table: "parents", SQL: "SELECT p.id FROM parents p"}, Relations: []*spec.Relation{{Name: "Rows", Holder: "Rows", View: leaf}}}
	component := &spec.Component{Name: "Public", RootView: root, Settings: &spec.Settings{Mutation: "patch", DefaultConnector: "main"}, Parameters: []*spec.Parameter{{Name: "Parents", TypeExpr: "[]*Parent", Cardinality: "many", Source: spec.BindSource{Kind: "body", Name: "data"}}}}
	require.NoError(t, refiner.RefineRoot(ctx, component, nil, nil))
	root.Auxiliary, leaf.Auxiliary = true, true
	declaration := spec.BorrowedSQLRow{BodyPath: "Parents/Rows", OwnerBodyPath: "Rows", OwnerName: "Private", Package: "example.com/api", Type: "Row"}
	return &Result{Component: component, Source: &Source{Name: "Public", ColumnRefiner: refiner}}, leaf, declaration
}

func TestBorrowedAuxiliaryNestedNullComparisonClone(t *testing.T) {
	result, leaf, declaration := borrowedAuxiliaryNullComparisonFixture(t)
	withoutPolicy, err := borrowedLeafContract(context.Background(), result, leaf, declaration)
	require.NoError(t, err)
	leaf.NestedNullPolicy = "skip-auxiliary"
	before, err := json.Marshal(result.Component)
	require.NoError(t, err)
	withPolicy, err := borrowedLeafContract(context.Background(), result, leaf, declaration)
	require.NoError(t, err)
	require.NoError(t, gen.CompareBorrowedLeafContracts(withoutPolicy, withPolicy))
	require.Equal(t, "skip-auxiliary", leaf.NestedNullPolicy)
	after, err := json.Marshal(result.Component)
	require.NoError(t, err)
	require.JSONEq(t, string(before), string(after), "comparison mutated canonical policy, source or native fields")
	// The public generic helper retains its strict detached-root semantics.
	_, err = gen.BorrowedLeafContractFor(result.Component, leaf, declaration.Package, declaration.Type, "main", "", "main", "records")
	require.ErrorContains(t, err, "nested_null_policy requires")
}

func TestBorrowedAuxiliaryNestedNullComparisonRejectsMisuse(t *testing.T) {
	for _, name := range []string{"writable", "one", "unknown", "initial-validation", "root-policy", "root", "derived", "get", "off-path", "current"} {
		t.Run(name, func(t *testing.T) {
			result, leaf, declaration := borrowedAuxiliaryNullComparisonFixture(t)
			leaf.NestedNullPolicy = "skip-auxiliary"
			switch name {
			case "writable":
				leaf.Auxiliary = false
			case "one":
				leaf.Cardinality = spec.CardinalityOne
			case "unknown":
				leaf.NestedNullPolicy = "unknown"
			case "initial-validation":
				leaf.NestedNullPolicy = "initial-validation"
			case "root-policy":
				leaf.RootNullPolicy = "skip-auxiliary"
			case "root":
				result.Component.RootView = leaf
			case "derived":
				result.Component.RootView.Relations[0].Kind = spec.RelationKindDerived
			case "get":
				result.Component.Routes = []*spec.Route{{Method: "GET"}}
			case "off-path", "current":
				result.Component.RootView.Relations = nil
				result.Component.Views = []*spec.View{leaf}
				if name == "current" {
					result.Component.Parameters = append(result.Component.Parameters, &spec.Parameter{Name: "CurrentRows", TypeExpr: "[]*Row", Source: spec.BindSource{Kind: "view", Name: "Rows"}})
				}
			}
			before, err := json.Marshal(result.Component)
			require.NoError(t, err)
			_, err = borrowedLeafContract(context.Background(), result, leaf, declaration)
			require.Error(t, err, "invalid canonical role policy was normalized")
			after, err := json.Marshal(result.Component)
			require.NoError(t, err)
			require.JSONEq(t, string(before), string(after))
		})
	}
}

func TestBorrowedAuxiliaryNestedNullResolvedSourcesAndPhysicalGuards(t *testing.T) {
	for _, encoding := range []string{"inline", "uri", "embed"} {
		for _, sourceKind := range []string{"direct", "join", "cte", "computed", "duplicate", "unresolved"} {
			t.Run(encoding+"/"+sourceKind, func(t *testing.T) {
				result, leaf, declaration := borrowedAuxiliaryNullComparisonFixture(t)
				leaf.NestedNullPolicy = "skip-auxiliary"
				sql := leaf.Source.SQL
				switch sourceKind {
				case "join":
					sql += " JOIN parents p ON p.id=r.parent_id"
				case "cte":
					sql = "WITH selected AS (SELECT * FROM records) SELECT r.id,r.parent_id,r.name FROM selected r"
				case "computed":
					sql = "SELECT r.id,r.parent_id,r.name||'' AS name FROM records r"
				case "duplicate":
					sql = "SELECT r.id,r.parent_id,r.name,r.name FROM records r"
				}
				resources, err := resource.New().WithDefault(fstest.MapFS{"records.sql": {Data: []byte(sql)}})
				require.NoError(t, err)
				result.Source.Resources = resources
				leaf.Source.SQL = sql
				switch encoding {
				case "uri":
					leaf.Source.SQL, leaf.Source.URI = "", "records.sql"
				case "embed":
					leaf.Source.SQL = "${embed:records.sql}"
					leaf.Source.Embeds = []*spec.EmbeddedSQLRef{{Path: "records.sql", Raw: leaf.Source.SQL}}
				}
				if sourceKind == "unresolved" {
					leaf.Source.SQL, leaf.Source.URI = "", "missing.sql"
				}
				before, err := json.Marshal(result.Component)
				require.NoError(t, err)
				_, err = borrowedLeafContract(context.Background(), result, leaf, declaration)
				if sourceKind == "direct" {
					require.NoError(t, err)
				} else {
					require.Error(t, err, "physical source proof bypassed by role normalization")
				}
				after, marshalErr := json.Marshal(result.Component)
				require.NoError(t, marshalErr)
				require.JSONEq(t, string(before), string(after))
				data, readErr := fs.ReadFile(resources, "records.sql")
				require.NoError(t, readErr)
				require.Equal(t, sql, string(data))
			})
		}
	}
}

func TestBorrowedAuxiliaryNestedNullRetainsComparisonAuthority(t *testing.T) {
	result, leaf, declaration := borrowedAuxiliaryNullComparisonFixture(t)
	leaf.NestedNullPolicy = "skip-auxiliary"
	actual, err := borrowedLeafContract(context.Background(), result, leaf, declaration)
	require.NoError(t, err)
	for _, name := range []string{"physical", "omission", "package", "name", "connector", "schema", "table", "fields", "type", "tag", "has", "wrapper", "root-policy", "initial-validation", "cardinality", "writer-policy"} {
		t.Run(name, func(t *testing.T) {
			changed := (&gen.BorrowedRowAuthority{Expected: actual}).Clone().Expected
			switch name {
			case "physical":
				changed.Physical[0] ^= 1
			case "omission":
				changed.WriterOmitEmpty = !changed.WriterOmitEmpty
			case "package":
				changed.Package = "example.com/other"
			case "name":
				changed.Name = "OtherRow"
			case "connector":
				changed.Connector = "other"
			case "schema":
				changed.Schema = "other"
			case "table":
				changed.Table = "parents"
			case "fields":
				changed.Fields = changed.Fields[1:]
			case "type":
				changed.Fields[0].Type = "bool"
			case "tag":
				changed.Fields[0].Tag += ` json:"other"`
			case "has":
				changed.MarkerFields[0].Name = "Other"
			case "wrapper":
				changed.Wrapper = "[]example.com/api.Row"
			case "root-policy":
				changed.Metadata.RootNullPolicy = "initial-validation"
			case "initial-validation":
				changed.Metadata.NestedNullPolicy = "initial-validation"
			case "cardinality":
				changed.Metadata.Cardinality = spec.CardinalityOne
			case "writer-policy":
				changed.Metadata.WriterActionPolicy = "insert-delete"
			}
			require.Error(t, gen.CompareBorrowedLeafContracts(actual, changed), "nonexception contract authority difference concealed")
		})
	}
	// The same fresh native field constraint application still rejects forged PK
	// metadata after deriving the comparison with the role-policy exception active.
	facts, err := result.Source.ColumnRefiner.PhysicalSourceConstraints(context.Background(), "main", leaf)
	require.NoError(t, err)
	forged := (&gen.BorrowedRowAuthority{Expected: actual}).Clone().Expected
	forged.Fields[0].Tag = strings.Replace(forged.Fields[0].Tag, ",primaryKey", ",otherPrimaryKey", 1)
	require.NotEqual(t, actual.Fields[0].Tag, forged.Fields[0].Tag)
	require.Error(t, applyBorrowedPhysicalFacts(&forged, facts))
}

func TestBorrowedResolvedResourceAuthority(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t, sqlite.WithDSN(filepath.Join(t.TempDir(), "resolved-authority.db")))
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER PRIMARY KEY)", "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,parent_id INTEGER REFERENCES parents(id),name TEXT)"))
	r := column.New(column.Connections{"main": db.DB})
	const direct = "SELECT r.id AS record_key,r.parent_id AS parent_key,r.name FROM records r"
	declaration := spec.BorrowedSQLRow{BodyPath: "Rows", OwnerBodyPath: "Rows", OwnerName: "Private", Package: "example.com/api", Type: "Row"}
	makeResult := func(t *testing.T, encoding, sql string) *Result {
		t.Helper()
		resources, err := resource.New().WithDefault(fstest.MapFS{"queries/records.sql": {Data: []byte(sql)}})
		require.NoError(t, err)
		values, err := constant.New(map[string]string{"folder": "queries"})
		require.NoError(t, err)
		source := &spec.ViewSource{Table: "records", SQL: sql}
		switch encoding {
		case "uri":
			source.SQL, source.URI = "", "${folder}/records.sql"
		case "embed":
			source.SQL = "${embed:queries/records.sql}"
			source.Embeds = []*spec.EmbeddedSQLRef{{Path: "${folder}/records.sql", Raw: source.SQL}}
		}
		view := &spec.View{Name: "Rows", TypeName: "Row", Source: source}
		component := &spec.Component{Name: "Private", RootView: view, Settings: &spec.Settings{Mutation: "patch", DefaultConnector: "main"}, Parameters: []*spec.Parameter{{Name: "Rows", TypeExpr: "[]*Row", Cardinality: "many", Source: spec.BindSource{Kind: "body", Name: "data"}}}}
		require.NoError(t, r.RefineRoot(ctx, component, resources, &column.TemplateInput{Const: values}))
		return &Result{Component: component, Source: &Source{Name: "Private", Resources: resources, Const: values, ColumnRefiner: r}}
	}
	var expected gen.BorrowedLeafContract
	for _, encoding := range []string{"inline", "uri", "embed"} {
		t.Run("direct_alias/"+encoding, func(t *testing.T) {
			result := makeResult(t, encoding, direct)
			view := result.Component.RootView
			before := view.Clone()
			resourceBefore, err := fs.ReadFile(result.Source.Resources, "queries/records.sql")
			require.NoError(t, err)
			actual, err := borrowedLeafContract(ctx, result, view, declaration)
			require.NoError(t, err)
			require.Equal(t, before, view, "comparison changed authored provenance or discovered columns")
			resourceAfter, err := fs.ReadFile(result.Source.Resources, "queries/records.sql")
			require.NoError(t, err)
			require.Equal(t, resourceBefore, resourceAfter)
			if encoding == "inline" {
				expected = actual
			} else {
				require.NoError(t, gen.CompareBorrowedLeafContracts(expected, actual))
			}
			var facts column.PhysicalSourceConstraints
			require.NoError(t, json.Unmarshal(actual.Physical, &facts))
			require.Equal(t, "id", facts.Projection[0].Physical)
			require.Equal(t, "record_key", facts.Projection[0].Result)
			view.Columns[1].Name = "ExternalParentKey"
			before = view.Clone()
			_, err = borrowedLeafContract(ctx, result, view, declaration)
			require.NoError(t, err)
			require.Equal(t, before, view)
		})
	}
	const unaliased = "SELECT r.id,r.parent_id,r.name FROM records r"
	for name, query := range map[string]string{
		"join":      unaliased + " JOIN parents p ON p.id=r.parent_id",
		"cte":       "WITH selected AS (SELECT * FROM records) SELECT r.id,r.parent_id,r.name FROM selected r",
		"computed":  "SELECT r.id,r.parent_id,r.name||'' AS name FROM records r",
		"duplicate": "SELECT r.id,r.parent_id,r.name,r.name FROM records r",
	} {
		for _, encoding := range []string{"inline", "uri", "embed"} {
			t.Run(name+"/"+encoding, func(t *testing.T) {
				result := makeResult(t, encoding, unaliased)
				// Keep genuine native columns from the successful direct discovery:
				// resource structure must independently reject matching row metadata.
				resources, err := resource.New().WithDefault(fstest.MapFS{"queries/records.sql": {Data: []byte(query)}})
				require.NoError(t, err)
				result.Source.Resources = resources
				view := result.Component.RootView
				if encoding == "inline" {
					view.Source.SQL = query
				}
				before := view.Clone()
				_, err = borrowedLeafContract(ctx, result, view, declaration)
				if name == "join" || name == "cte" {
					require.ErrorContains(t, err, "physical projection requires one direct unqualified source")
				} else {
					require.ErrorContains(t, err, "physical projection duplicate/computed output")
				}
				require.Equal(t, before, view)
			})
		}
	}
	for name, query := range map[string]string{
		"join": unaliased + " JOIN parents p ON p.id=r.parent_id",
		"cte":  "WITH selected AS (SELECT * FROM records) SELECT r.id,r.parent_id,r.name FROM selected r",
	} {
		for _, encoding := range []string{"inline", "uri", "embed"} {
			t.Run("fresh_discovery/"+name+"/"+encoding, func(t *testing.T) {
				result := makeResult(t, encoding, query)
				_, err := borrowedLeafContract(ctx, result, result.Component.RootView, declaration)
				require.ErrorContains(t, err, "physical projection requires one direct unqualified source")
			})
		}
	}
	for _, missing := range []string{"uri", "embed"} {
		t.Run("missing_resource/"+missing, func(t *testing.T) {
			result := makeResult(t, missing, direct)
			empty, err := resource.New().WithDefault(fstest.MapFS{})
			require.NoError(t, err)
			result.Source.Resources = empty
			_, err = borrowedLeafContract(ctx, result, result.Component.RootView, declaration)
			require.ErrorContains(t, err, "read SQL resource")
		})
		t.Run("missing_resource_fs/"+missing, func(t *testing.T) {
			result := makeResult(t, missing, direct)
			result.Source.Resources = nil
			_, err := borrowedLeafContract(ctx, result, result.Component.RootView, declaration)
			require.ErrorContains(t, err, "resource filesystem is required")
		})
	}
}

func TestBorrowedSQLRowAuthoredIntentAndSchemaRequired(t *testing.T) {
	declaration := spec.BorrowedSQLRow{BodyPath: "Rows/Records", Package: "example.com/app/api", Type: "Row", OwnerURI: "private/Private.dql", OwnerName: "Private", OwnerBodyPath: "Rows"}
	inherited := &Result{Component: &spec.Component{Settings: &spec.Settings{Generation: &spec.GenerationSettings{BorrowedSQLRows: []spec.BorrowedSQLRow{declaration}}}}}
	if err := NewCompiler().admitBorrowedSQLRows(context.Background(), inherited); err == nil || !strings.Contains(err.Error(), "inherited settings") {
		t.Fatalf("inherited intent accepted: %v", err)
	}
	authored := &Result{Source: &Source{}, Component: inherited.Component, AuthoredBorrowedSQLRows: []spec.BorrowedSQLRow{declaration}}
	if err := NewCompiler().admitBorrowedSQLRows(context.Background(), authored); err == nil || !strings.Contains(err.Error(), "current SQL schema") {
		t.Fatalf("schema-less intent accepted: %v", err)
	}
	declarations := authoredBorrowedSQLRows(inherited.Component)
	declarations[0].OwnerURI = "different.dql"
	if inherited.Component.Settings.Generation.BorrowedSQLRows[0].OwnerURI != declaration.OwnerURI {
		t.Fatal("authored declaration alias mutated")
	}
	ordinary := &Result{Component: &spec.Component{Settings: &spec.Settings{}}}
	if err := NewCompiler().admitBorrowedSQLRows(context.Background(), ordinary); err != nil {
		t.Fatal("ordinary generation changed", err)
	}
}

func TestBorrowedSQLRowExactBodyGraphSlot(t *testing.T) {
	leaf := &spec.View{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/source", Name: "Public"}, Namespace: "Records", Name: "Records", Auxiliary: true, Source: &spec.ViewSource{Table: "records"}}
	root := &spec.View{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/source", Name: "Public"}, Name: "Public", Relations: []*spec.Relation{{Name: "Records", Holder: "Records", View: leaf}}}
	c := &spec.Component{Name: "Public", RootView: root, Parameters: []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "body", Name: "data"}}}}
	got, graph, err := resolveBorrowedBodySlot(c, "Rows/Records")
	if err != nil || got != leaf || len(graph) != 2 {
		t.Fatal("canonical nested slot", graph, err)
	}
	if !leaf.Auxiliary {
		t.Fatal("semantic role mutated")
	}
	for _, path := range []string{"Public/Records", "Rows", "Rows/records", "Rows/Records/Unknown"} {
		if _, _, err := resolveBorrowedBodySlot(c, path); err == nil {
			t.Fatal("nonexact/parent slot accepted", path)
		}
	}
	root.Relations = append(root.Relations, &spec.Relation{Name: "Other", Holder: "Records", View: leaf.Clone()})
	if _, _, err := resolveBorrowedBodySlot(c, "Rows/Records"); err == nil {
		t.Fatal("ambiguous holder accepted")
	}
	root.Relations = root.Relations[:1]
	root.Relations[0].View = root
	if _, _, err := resolveBorrowedBodySlot(c, "Rows/Records"); err == nil {
		t.Fatal("cyclic body accepted")
	}
}

func TestBorrowedConsumedSourceAndResources(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "Public.dql")
	sql := filepath.Join(root, "embedded.sql")
	text := "#setting($_ = $borrow_sql_row('Rows','example.com/api','Row','Private.dql','Private','Rows'))\nSELECT r.* FROM records r"
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sql, []byte("SELECT 1"), 0600); err != nil {
		t.Fatal(err)
	}
	source := &Source{Path: path, Text: text}
	consumed, err := sealBorrowedSourcePackage(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = validateBorrowedSourceText(source, consumed); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, path, text string }{{"removed declaration", path, "SELECT r.* FROM records r"}, {"same-size source", path, strings.Replace(text, "records r", "recordx r", 1)}, {"same-size embed", sql, "SELECT 2"}} {
		t.Run(tc.name, func(t *testing.T) {
			original, err := os.ReadFile(tc.path)
			if err != nil {
				t.Fatal(err)
			}
			defer os.WriteFile(tc.path, original, 0600)
			info, _ := os.Stat(tc.path)
			if err = os.WriteFile(tc.path, []byte(tc.text), 0600); err != nil {
				t.Fatal(err)
			}
			if err = os.Chtimes(tc.path, info.ModTime(), info.ModTime()); err != nil {
				t.Fatal(err)
			}
			current, err := sealBorrowedSourcePackage(root)
			if err != nil {
				t.Fatal(err)
			}
			if err = validateBorrowedConsumedFiles(consumed, current); err == nil {
				t.Fatal("consumed source/resource drift accepted")
			}
			if tc.path == path && validateBorrowedSourceText(source, current) == nil {
				t.Fatal("different consumed DQL accepted")
			}
		})
	}
}

func TestBorrowedValidatedOwnerResourceChangedBeforeClosure(t *testing.T) {
	root := t.TempDir()
	sqlDir := filepath.Join(root, "sql")
	if err := os.Mkdir(sqlDir, 0755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(sqlDir, "owned.sql")
	if err := os.WriteFile(path, []byte("SELECT 1"), 0600); err != nil {
		t.Fatal(err)
	}
	proof := &gen.BorrowedRowAuthority{OwnerName: "Private", OwnerDirectory: root, Expected: gen.BorrowedLeafContract{Name: "Row"}}
	validated, err := gen.SealBorrowedAuthorityFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = gen.RetainBorrowedSeal(proof, validated); err != nil {
		t.Fatal(err)
	}
	header := "// Code generated by Datly for Private; DO NOT EDIT.\npackage api\n"
	if err = os.WriteFile(filepath.Join(root, "views.go"), []byte(header+"type Row struct{}\ntype RowHas struct{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "resources.go"), []byte(header+"//go:embed sql/owned.sql\nvar fs string\n"), 0600); err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(path)
	if err = os.WriteFile(path, []byte("SELECT 2"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	if err = sealBorrowedOwnerFiles(proof); err == nil || !strings.Contains(err.Error(), "between validated artifact") {
		t.Fatalf("changed owner resource became sealed baseline: %v", err)
	}
}

func TestBorrowedActualNativeFieldsAndIndependentPhysicalFacts(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "exclusive-field-admission.db")
	db := sqlite.New(t, sqlite.WithDSN(path))
	t.Logf("EXCLUSIVE_SQLITE owner=%s path=%s", t.Name(), path)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER PRIMARY KEY)", "CREATE TABLE alternate(id INTEGER PRIMARY KEY)", "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,a INTEGER REFERENCES parents(id),b INTEGER,name TEXT)", "CREATE UNIQUE INDEX pair_unique ON records(a,b)"))
	r := column.New(column.Connections{"main": db.DB})
	v := &spec.View{Name: "Rows", TypeName: "Row", Source: &spec.ViewSource{Table: "records", SQL: "SELECT r.id AS record_key,r.a AS parent_key,r.b,r.name FROM records r"}}
	c := &spec.Component{Name: "Private", RootView: v, Settings: &spec.Settings{Mutation: "patch", DefaultConnector: "main"}}
	require.NoError(t, r.RefineRoot(ctx, c, nil, nil))
	contract := func(view *spec.View) gen.BorrowedLeafContract {
		t.Helper()
		a, err := gen.BorrowedLeafContractFor(c, view, "example.com/api", "Row", "main", "", "main", "records")
		require.NoError(t, err)
		return a
	}
	facts, err := r.PhysicalSourceConstraints(ctx, "main", v)
	require.NoError(t, err)
	a := contract(v)
	require.NoError(t, applyBorrowedPhysicalFacts(&a, facts))
	require.NotEmpty(t, a.Physical)
	// Verify SQL-result aliases against the real native generator, then its outer Go rename.
	renamed := v.Clone()
	renamed.Columns[1].Name = "ExternalParentKey"
	renamedFacts, err := r.PhysicalSourceConstraints(ctx, "main", renamed)
	require.NoError(t, err)
	renamedContract := contract(renamed)
	require.NoError(t, applyBorrowedPhysicalFacts(&renamedContract, renamedFacts))
	var copied column.PhysicalSourceConstraints
	require.NoError(t, json.Unmarshal(a.Physical, &copied))
	require.Equal(t, "id", copied.Projection[0].Physical)
	require.Equal(t, "record_key", copied.Projection[0].Result)
	for _, mutation := range []string{"primary", "auto", "unique", "source", "result_alias", "ref_table", "ref_schema", "invented_ref"} {
		t.Run(mutation, func(t *testing.T) {
			broken := a
			broken.Fields = append([]gen.Field(nil), a.Fields...)
			idx := 0
			switch mutation {
			case "primary":
				broken.Fields[0].Tag = strings.Replace(broken.Fields[0].Tag, ",primaryKey", "", 1)
			case "auto":
				if strings.Contains(broken.Fields[0].Tag, ",autoincrement") {
					broken.Fields[0].Tag = strings.Replace(broken.Fields[0].Tag, ",autoincrement", "", 1)
				} else {
					broken.Fields[0].Tag = strings.Replace(broken.Fields[0].Tag, `sqlx:"id`, `sqlx:"id,autoincrement=true`, 1)
				}
			case "unique":
				idx = 2
				broken.Fields[2].Tag = strings.Replace(broken.Fields[2].Tag, `sqlx:"b`, `sqlx:"b,unique`, 1)
			case "source":
				broken.Fields[0].Tag = strings.Replace(broken.Fields[0].Tag, `sqlx:"id`, `sqlx:"a`, 1)
			case "result_alias":
				broken.Fields[0].Tag = strings.Replace(broken.Fields[0].Tag, "record_key", "wrong_alias", 1)
			case "ref_table":
				idx = 1
				broken.Fields[1].Tag = strings.Replace(broken.Fields[1].Tag, "refTable=parents", "refTable=alternate", 1)
			case "ref_schema":
				idx = 1
				broken.Fields[1].Tag = strings.Replace(broken.Fields[1].Tag, "refTable=parents", "refDb=other,refTable=parents", 1)
			case "invented_ref":
				idx = 2
				broken.Fields[2].Tag = strings.Replace(broken.Fields[2].Tag, `sqlx:"b`, `sqlx:"b,refTable=parents,refColumn=id`, 1)
			}
			require.NotEqual(t, a.Fields[idx].Tag, broken.Fields[idx].Tag, "mutation must change native field")
			require.Error(t, applyBorrowedPhysicalFacts(&broken, facts))
		})
	}
	// Native target changes remain visible even when old emitted/authored tags stay fixed.
	require.NoError(t, db.ExecStatements(ctx, "DROP INDEX pair_unique", "DROP TABLE records", "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,a INTEGER REFERENCES alternate(id),b INTEGER,name TEXT)", "CREATE UNIQUE INDEX pair_unique ON records(a,b)"))
	changed, err := r.PhysicalSourceConstraints(ctx, "main", v)
	require.NoError(t, err)
	require.ErrorContains(t, applyBorrowedPhysicalFacts(&a, changed), "reference override")
	require.NoError(t, db.ExecStatements(ctx, "DROP TABLE records", "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,a INTEGER REFERENCES parents(id),b INTEGER,name TEXT)", "CREATE INDEX pair_unique ON records(a,b)"))
	changed, err = r.PhysicalSourceConstraints(ctx, "main", v)
	require.NoError(t, err)
	b := contract(v)
	require.NoError(t, applyBorrowedPhysicalFacts(&b, changed))
	require.False(t, bytes.Equal(a.Physical, b.Physical))
	require.Error(t, gen.CompareBorrowedLeafContracts(a, b))
	// Authority clones own evidence bytes.
	proof := (&gen.BorrowedRowAuthority{Expected: a}).Clone()
	proof.Expected.Physical[0] ^= 1
	require.NotEqual(t, proof.Expected.Physical[0], a.Physical[0])
}

func TestBorrowedCompiledAuthorityFreshNativeKeyDrift(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	const module = "github.com/viant/datly/borrowedkeyfixture"
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	path := filepath.Join(root, "exclusive-boundary.db")
	db := sqlite.New(t, sqlite.WithDSN(path))
	t.Logf("EXCLUSIVE_SQLITE owner=%s path=%s", t.Name(), path)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER PRIMARY KEY)", "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,parent_id INTEGER REFERENCES parents(id),name TEXT)", "CREATE UNIQUE INDEX pair_unique ON records(parent_id,name)"))
	r := column.New(column.Connections{"main": db.DB})
	sourceDir := filepath.Join(root, "source")
	privateDir := filepath.Join(sourceDir, "private")
	require.NoError(t, os.MkdirAll(privateDir, 0755))
	owner := `#package('` + module + `/api')
#setting($_ = $route('/private','PATCH'))
#setting($_ = $file_prefix('private_'))
#setting($_ = $input_type('PrivateInput'))
#setting($_ = $output_type('PrivateOutput'))
#define($_ = $Rows<[]*Row>(body/data).Cardinality('Many').Required())
SELECT Records.*,type(Records,'Row') FROM records Records`
	borrower := `#package('` + module + `/api')
#setting($_ = $route('/public','PATCH'))
#setting($_ = $file_prefix('public_'))
#setting($_ = $input_type('PublicInput'))
#setting($_ = $output_type('PublicOutput'))
#setting($_ = $borrow_sql_row('Rows/Records','` + module + `/api','Row','private/Private.dql','Private','Rows'))
#define($_ = $Rows<[]*PublicView>(body/data).Cardinality('Many').Required())
SELECT p.*,Records.*,type(p,'PublicView'),type(Records,'Row') FROM (parents) p JOIN (records) Records ON Records.parent_id=p.id`
	makeSource := func(scope, name, path, text string) *Source {
		require.NoError(t, os.WriteFile(path, []byte(text), 0600))
		return &Source{Scope: scope, Name: name, Path: path, Text: text, Connector: "main", ColumnRefiner: r, Types: typecatalog.NewCatalog()}
	}
	ownerSource := makeSource(module+"/source/private", "Private", filepath.Join(privateDir, "Private.dql"), owner)
	_, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Source: ownerSource, Destination: root})
	require.NoError(t, err)
	borrowerSource := makeSource(module+"/source", "Public", filepath.Join(sourceDir, "Public.dql"), borrower)
	generated, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Source: borrowerSource, Destination: root})
	require.NoError(t, err)
	require.Len(t, generated.Result.Plan.BorrowedRows, 1)
	proof := generated.Result.Plan.BorrowedRows[0]
	require.NoError(t, proof.ValidateSchema())
	retained := append([]byte(nil), proof.Expected.Physical...)
	// These changes keep row/Has/descriptor/source bytes unchanged, so only fresh
	// independent native constraint evidence can reject the retained authority.
	for _, queries := range [][]string{{"DROP INDEX pair_unique", "CREATE INDEX pair_unique ON records(parent_id,name)"}, {"DROP INDEX pair_unique", "CREATE UNIQUE INDEX pair_unique ON records(name,parent_id)"}, {"DROP INDEX pair_unique"}} {
		require.NoError(t, db.ExecStatements(ctx, queries...))
		require.ErrorContains(t, proof.ValidateSchema(), "leaf contracts differ")
		require.Equal(t, retained, []byte(proof.Expected.Physical))
	}
	require.NoError(t, db.ExecStatements(ctx, "CREATE UNIQUE INDEX pair_unique ON records(parent_id,name)"))
	require.NoError(t, proof.ValidateSchema())
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE alternate(id INTEGER PRIMARY KEY)", "DROP INDEX pair_unique", "DROP TABLE records", "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,parent_id INTEGER REFERENCES alternate(id),name TEXT)", "CREATE UNIQUE INDEX pair_unique ON records(parent_id,name)"))
	require.Error(t, proof.ValidateSchema())
	require.Equal(t, retained, []byte(proof.Expected.Physical))
}

func TestBorrowedAuxiliaryNestedNullFreshNativeAndPolicyDrift(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	const module = "github.com/viant/datly/borrowedkeyfixture"
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	path := filepath.Join(root, "exclusive-boundary.db")
	db := sqlite.New(t, sqlite.WithDSN(path))
	t.Logf("EXCLUSIVE_SQLITE owner=%s path=%s", t.Name(), path)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER PRIMARY KEY)", "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,parent_id INTEGER REFERENCES parents(id),name TEXT)", "CREATE UNIQUE INDEX pair_unique ON records(parent_id,name)"))
	r := column.New(column.Connections{"main": db.DB})
	sourceDir := filepath.Join(root, "source")
	privateDir := filepath.Join(sourceDir, "private")
	require.NoError(t, os.MkdirAll(privateDir, 0755))
	owner := `#package('` + module + `/api')
#setting($_ = $route('/private','PATCH'))
#setting($_ = $file_prefix('private_'))
#setting($_ = $input_type('PrivateInput'))
#setting($_ = $output_type('PrivateOutput'))
#define($_ = $Rows<[]*Row>(body/data).Cardinality('Many').Required())
SELECT Records.*,type(Records,'Row') FROM records Records`
	borrower := `#package('` + module + `/api')
#setting($_ = $route('/public','PATCH'))
#setting($_ = $file_prefix('public_'))
#setting($_ = $input_type('PublicInput'))
#setting($_ = $output_type('PublicOutput'))
#setting($_ = $borrow_sql_row('Rows/Records','` + module + `/api','Row','private/Private.dql','Private','Rows'))
#define($_ = $Rows<[]*PublicView>(body/data).Cardinality('Many').Required())
SELECT p.*,Records.*,type(p,'PublicView'),type(Records,'Row'),nested_null_policy(Records,'skip-auxiliary') FROM (parents) p JOIN (records) Records ON Records.parent_id=p.id`
	makeSource := func(scope, name, path, text string) *Source {
		require.NoError(t, os.WriteFile(path, []byte(text), 0600))
		return &Source{Scope: scope, Name: name, Path: path, Text: text, Connector: "main", ColumnRefiner: r, Types: typecatalog.NewCatalog()}
	}
	ownerSource := makeSource(module+"/source/private", "Private", filepath.Join(privateDir, "Private.dql"), owner)
	_, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Source: ownerSource, Destination: root})
	require.NoError(t, err)
	borrowerSource := makeSource(module+"/source", "Public", filepath.Join(sourceDir, "Public.dql"), borrower)
	generated, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Source: borrowerSource, Destination: root})
	require.NoError(t, err)
	require.Len(t, generated.Result.Plan.BorrowedRows, 1)
	proof := generated.Result.Plan.BorrowedRows[0]
	require.NoError(t, proof.ValidateSchema())
	// Existing proofs reparse captured Source.Text. A new authored policy must
	// be validated on fresh admission without changing the publication forest.
	changedSource := *borrowerSource
	changedSource.Text = strings.Replace(borrower, "skip-auxiliary", "unknown", 1)
	require.NoError(t, os.WriteFile(borrowerSource.Path, []byte(changedSource.Text), 0600))
	snapshot := func() map[string]struct {
		Mode  fs.FileMode
		Bytes string
	} { result := map[string]struct {
		Mode  fs.FileMode
		Bytes string
	}{}; require.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		var data []byte
		if !entry.IsDir() {
			data, err = os.ReadFile(path)
			if err != nil {
				return err
			}
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		result[rel] = struct {
			Mode  fs.FileMode
			Bytes string
		}{info.Mode(), string(data)}
		return nil
	})); return result }
	before := snapshot()
	_, err = (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Source: &changedSource, Destination: root})
	require.ErrorContains(t, err, "nested_null_policy")
	require.Equal(t, before, snapshot())
	require.NoError(t, os.WriteFile(borrowerSource.Path, []byte(borrower), 0600))
	require.NoError(t, proof.ValidateSchema())
	retained := append([]byte(nil), proof.Expected.Physical...)
	// These changes keep row/Has/descriptor/source bytes unchanged, so only fresh
	// independent native constraint evidence can reject the retained authority.
	for _, queries := range [][]string{{"DROP INDEX pair_unique", "CREATE INDEX pair_unique ON records(parent_id,name)"}, {"DROP INDEX pair_unique", "CREATE UNIQUE INDEX pair_unique ON records(name,parent_id)"}, {"DROP INDEX pair_unique"}} {
		require.NoError(t, db.ExecStatements(ctx, queries...))
		require.ErrorContains(t, proof.ValidateSchema(), "leaf contracts differ")
		require.Equal(t, retained, []byte(proof.Expected.Physical))
	}
	require.NoError(t, db.ExecStatements(ctx, "CREATE UNIQUE INDEX pair_unique ON records(parent_id,name)"))
	require.NoError(t, proof.ValidateSchema())
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE alternate(id INTEGER PRIMARY KEY)", "DROP INDEX pair_unique", "DROP TABLE records", "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,parent_id INTEGER REFERENCES alternate(id),name TEXT)", "CREATE UNIQUE INDEX pair_unique ON records(parent_id,name)"))
	require.Error(t, proof.ValidateSchema())
	require.Equal(t, retained, []byte(proof.Expected.Physical))
}
