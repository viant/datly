package transcribe

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
	hc "github.com/viant/datly/transcribe/handler/compiler"
	"github.com/viant/datly/transcribe/internal/handlergen"
	"github.com/viant/datly/typecatalog"
)

func TestBorrowedAncestorPresenceRealTargetRequestsWithWritableSibling(t *testing.T) {
	for _, target := range []string{"get", "go-direct", "velty", "go-universal"} {
		t.Run(target, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			const module = "github.com/viant/datly/borrowedpresencefixture"
			(testharness.GeneratedModule{Path: module}).Write(t, root)
			db := sqlite.New(t, sqlite.WithDSN(filepath.Join(root, "exclusive-presence.db")))
			if err := db.ExecStatements(ctx, "CREATE TABLE roots(id INTEGER PRIMARY KEY)", "CREATE TABLE parents(id INTEGER PRIMARY KEY,root_id INTEGER REFERENCES roots(id))", "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,parent_id INTEGER REFERENCES parents(id),name TEXT)", "CREATE TABLE siblings(id INTEGER PRIMARY KEY,root_id INTEGER REFERENCES roots(id))"); err != nil {
				t.Fatal(err)
			}
			refiner := column.New(column.Connections{"main": db.DB})
			sourceDir := filepath.Join(root, "source")
			private := filepath.Join(sourceDir, "private")
			if err := os.MkdirAll(private, 0700); err != nil {
				t.Fatal(err)
			}
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
#setting($_ = $borrow_sql_row('Rows/Parents/Records','` + module + `/api','Row','private/Private.dql','Private','Rows'))
#define($_ = $Rows<[]*PublicView>(body/data).Cardinality('Many').Required())
#define($_ = $CurrentParents<?>(view/CurrentParents).Cardinality('Many') /* SELECT id,root_id FROM (parents) */)
#define($_ = $CurrentRecords<?>(view/CurrentRecords).Cardinality('Many') /* SELECT * FROM (records) */)
SELECT r.*,Parents.*,Records.*,Siblings.*,type(r,'PublicView'),type(Parents,'Parent'),type(Records,'Row') FROM (roots) r JOIN (SELECT * FROM (parents)) Parents ON Parents.root_id=r.id JOIN (records) Records ON Records.parent_id=Parents.id JOIN (siblings) Siblings ON Siblings.root_id=r.id`
			makeSource := func(name, path, text string) *Source {
				if err := os.WriteFile(path, []byte(text), 0600); err != nil {
					t.Fatal(err)
				}
				return &Source{Scope: module + "/source", Name: name, Path: path, Text: text, Connector: "main", ColumnRefiner: refiner, Types: typecatalog.NewCatalog()}
			}
			ownerSource := makeSource("Private", filepath.Join(private, "Private.dql"), owner)
			if _, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Source: ownerSource, Destination: root}); err != nil {
				t.Fatal(err)
			}
			source := makeSource("Public", filepath.Join(sourceDir, "Public.dql"), borrower)
			compiled, err := NewCompiler().Compile(ctx, source)
			if err != nil {
				t.Fatal(err)
			}
			if target == "get" {
				result, err := (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Compiled: compiled, Destination: root})
				if err != nil {
					t.Fatal(err)
				}
				for _, v := range result.Result.Plan.Views {
					if v.Ownership == gen.ViewGenerated && len(v.SetMarkerFields) != 0 {
						t.Fatal("GET manufactured borrowed ancestor markers")
					}
				}
				return
			}
			if compiled.Component.Settings == nil {
				compiled.Component.Settings = &spec.Settings{}
			}
			compiled.Component.Settings.Mutation = "patch"
			// Use an actual ordinary writable sibling so root marker-map membership
			// alone cannot distinguish the new exception's target.
			for _, relation := range compiled.Component.RootView.Relations {
				if relation.Holder == "Parents" && !relation.View.Auxiliary {
					t.Fatal("fixture parent must retain authored auxiliary authority")
				}
				if relation.Holder == "Siblings" {
					relation.View.Auxiliary = false
					relation.View.Source.Table = "siblings"
				}
			}
			if err := column.ApplyWriterMetadata(compiled.Component); err != nil {
				t.Fatal(err)
			}
			input, _, err := generationInput(root, "", compiled)
			if err != nil {
				t.Fatal(err)
			}
			initial, err := gen.New(input).Plan()
			if err != nil {
				t.Fatal(err)
			}
			derived, err := (&hc.Compiler{}).BuildInput(hc.Request{Component: input.Component, ViewBindings: input.ViewBindings, Operation: WritePatch}, initial.RootViewType)
			if err != nil {
				t.Fatal(err)
			}
			compiled.Component = derived.Component
			compiled.ViewBindings = derived.ViewBindings
			declarations, err := newDeclarationCompiler(compiled.Component, nil).compile()
			if err != nil {
				t.Fatal(err)
			}
			compiled.Declarations = declarations.generation
			input, _, err = generationInput(root, "", compiled)
			if err != nil {
				t.Fatal(err)
			}
			options := Options{Contracts: ContractsAuto, Handler: HandlerOptions{Target: HandlerGo, Operation: WritePatch, Input: derived.Input, Output: derived.Output, Currents: derived.Currents, Go: GoHandlerOptions{Execution: GoExecutionDirect}}}
			if target == "velty" {
				options.Handler.Target = HandlerVelty
			}
			if target == "go-universal" {
				options.Handler.Go.Execution = GoExecutionMutation
			}
			generation := handlergen.New(compiled.handlerContext(), &input, options)
			if err := generation.Prepare(); err != nil {
				t.Fatal(err)
			}
			plan, err := gen.New(input).Plan()
			if err != nil {
				t.Fatal(err)
			}
			rootID, _ := compiled.Component.RootView.Identity()
			var rootMarkers []string
			var parentMarkers []string
			for _, v := range plan.Views {
				if v.Identity == rootID {
					rootMarkers = v.SetMarkerFields
				}
				if v.Name == "Parent" {
					parentMarkers = v.SetMarkerFields
				}
			}
			has := func(values []string, want string) bool {
				for _, value := range values {
					if value == want {
						return true
					}
				}
				return false
			}
			if !has(rootMarkers, "Siblings") {
				t.Fatalf("ordinary writable sibling marker missing: %v", rootMarkers)
			}
			eligible := target == "go-universal"
			if has(rootMarkers, "Parents") != eligible || has(parentMarkers, "Records") != eligible {
				t.Fatalf("target=%s root=%v parent=%v", target, rootMarkers, parentMarkers)
			}
		})
	}
}
