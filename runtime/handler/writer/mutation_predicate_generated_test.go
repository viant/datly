package writer_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/exec"
	druntime "github.com/viant/datly/runtime"
	writer "github.com/viant/datly/runtime/handler/writer"
	fixture "github.com/viant/datly/runtime/handler/writer/testdata/nestedcriteria/generated"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/dml"
	viewprovider "github.com/viant/datly/sql/reader/provider"
	dtag "github.com/viant/datly/tag"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	xhandler "github.com/viant/xdatly/handler"
)

func guardPointer[T any](v T) *T { return &v }
func guardInput() *fixture.GuardInput {
	input := new(fixture.GuardInput)
	input.SetTenant("tenant")
	input.SetLedgerID(1)
	input.SetExpectedState("pending")
	input.SetExpectedHash("request")
	input.SetExpectedRevision(1)
	input.SetExpectedFingerprint("old")
	ledger := new(fixture.Ledger)
	ledger.SetTenant(guardPointer("tenant"))
	ledger.SetId(guardPointer(1))
	ledger.SetState(guardPointer("committed"))
	document := new(fixture.Document)
	document.SetTenant(guardPointer("tenant"))
	document.SetId(guardPointer(1))
	document.SetLedgerId(guardPointer(1))
	document.SetRevision(guardPointer(1))
	document.SetTitle(guardPointer("updated"))
	draft := new(fixture.Draft)
	draft.SetTenant(guardPointer("tenant"))
	draft.SetDocumentId(guardPointer(1))
	draft.SetRevision(guardPointer(1))
	draft.SetFingerprint(guardPointer("new"))
	draft.SetContent(guardPointer("updated"))
	document.SetDrafts(draft)
	ledger.SetDocuments([]*fixture.Document{document})
	input.SetComponent([]*fixture.Ledger{ledger})
	return input
}
func guardRuntime(ctx context.Context, t *testing.T, db *sql.DB) (*druntime.Runtime, exec.ComponentTarget) {
	t.Helper()
	holder := reflect.TypeFor[fixture.ComponentComponent]()
	field, _ := holder.FieldByName("Contract")
	tag, _, err := dtag.ParseComponent(field.Tag)
	if err != nil {
		t.Fatal(err)
	}
	component, err := (&bootstrap.RouteSource{HolderType: holder.Name(), FieldName: field.Name, PackageName: "generated", PackagePath: holder.PkgPath(), Tag: tag}).Resolve(reflect.TypeFor[fixture.GuardInput](), reflect.TypeFor[fixture.GuardOutput]())
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := writer.Compile(component, reflect.TypeFor[fixture.GuardInput](), reflect.TypeFor[fixture.GuardOutput](), "put")
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Root.MutationPredicateGroup == nil || *metadata.Root.MutationPredicateGroup != 7 {
		t.Fatal("root predicate group lost")
	}
	child := metadata.Root.Relations[0].Child.Relations[0].Child
	if child.MutationPredicateGroup == nil || *child.MutationPredicateGroup != 8 {
		t.Fatal("generated nested predicate group lost")
	}
	resources := resource.New()
	if err = resources.Register(fixture.ComponentDatlyResourceNamespace, fixture.ComponentDatlyResources); err != nil {
		t.Fatal(err)
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[fixture.GuardInput](), OutputType: reflect.TypeFor[fixture.GuardOutput](), Resources: resources})
	if err != nil {
		t.Fatal(err)
	}
	connector := &dsql.SQLComponent{DB: db}
	if err = connector.RegisterConnector("main", db); err != nil {
		t.Fatal(err)
	}
	views, err := viewprovider.New(viewprovider.Config{Dependencies: artifact.ViewDependencies, Input: artifact.Input, SQL: connector})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := writer.New(component, reflect.TypeFor[fixture.GuardInput](), reflect.TypeFor[fixture.GuardOutput](), "put")
	if err != nil {
		t.Fatal(err)
	}
	entry, err := artifact.Registration(registry.RegisteredComponent{Handler: handler, Providers: []locator.Provider{views}, DataSource: dml.Source{DB: db}})
	if err != nil {
		t.Fatal(err)
	}
	entry.Capabilities.Connector = connector
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry}, druntime.WithResources(resources))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Shutdown(context.WithoutCancel(ctx)); err != nil {
			t.Error(err)
		}
	})
	return runtime, exec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "PUT", Path: "/test/nested-criteria"}}
}

// The fixture is generated exclusively by stock transcribe from matching DQL.
// A drain-time trigger changes persisted child evidence after Previous. A native
// CAS conflict must roll back the preceding ledger/document updates and trigger.
func TestStockGeneratedNestedMutationPredicateNativeRollback(t *testing.T) {
	for _, scenario := range []string{"commit", "child-fingerprint-race", "child-revision-race", "root-state", "root-hash", "delete", "child-delete-stale"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := t.Context()
			db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "fixture.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ddl, err := os.ReadFile("testdata/nestedcriteria/schema.sql")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = db.ExecContext(ctx, string(ddl)); err != nil {
				t.Fatal(err)
			}
			if _, err = db.ExecContext(ctx, `INSERT INTO test_ledger VALUES('tenant',1,'pending','request');INSERT INTO test_documents VALUES('tenant',1,1,'original',1);INSERT INTO test_drafts VALUES('tenant',1,1,'old','original');`); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "child-fingerprint-race":
				_, err = db.ExecContext(ctx, `CREATE TRIGGER before_ledger_update BEFORE UPDATE ON test_ledger BEGIN UPDATE test_drafts SET fingerprint='changed'; END;`)
			case "child-revision-race":
				_, err = db.ExecContext(ctx, `CREATE TRIGGER before_ledger_update BEFORE UPDATE ON test_ledger BEGIN UPDATE test_drafts SET revision=9; END;`)
			}
			if err != nil {
				t.Fatal(err)
			}
			runtime, target := guardRuntime(ctx, t, db)
			input := guardInput()
			if scenario == "child-delete-stale" {
				input.SetExpectedFingerprint("stale")
			}
			if strings.Contains(scenario, "delete") {
				input.Component[0].Documents[0].Drafts.SetShouldDelete(true)
			}
			if scenario == "root-state" {
				input.SetExpectedState("failed")
			}
			if scenario == "root-hash" {
				input.SetExpectedHash("other")
			}
			_, err = runtime.InvokeComponent(ctx, exec.ComponentRequest{Target: target, Input: input})
			if scenario == "commit" || scenario == "delete" {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				if err == nil {
					t.Fatal("stale physical criteria committed")
				}
				var conflict *xhandler.Conflict
				if !errors.As(err, &conflict) {
					t.Fatalf("expected native conflict: %T %v", err, err)
				}
			}

			if scenario == "delete" {
				var remaining int
				if err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM test_drafts`).Scan(&remaining); err != nil {
					t.Fatal(err)
				}
				if remaining != 0 {
					t.Fatal("explicit child delete did not persist")
				}
				var state, title string
				var revision int
				if err = db.QueryRowContext(ctx, `SELECT l.state,d.title,d.revision FROM test_ledger l JOIN test_documents d ON d.tenant=l.tenant AND d.ledger_id=l.id`).Scan(&state, &title, &revision); err != nil {
					t.Fatal(err)
				}
				if state != "committed" || title != "updated" || revision != 2 {
					t.Fatal("delete parent/ledger did not commit")
				}
				return
			}

			var state, title, fingerprint, content string
			var documentRevision, draftRevision int
			if err = db.QueryRowContext(ctx, `SELECT l.state,d.title,d.revision,r.fingerprint,r.content,r.revision FROM test_ledger l JOIN test_documents d ON d.tenant=l.tenant AND d.ledger_id=l.id JOIN test_drafts r ON r.tenant=d.tenant AND r.document_id=d.id`).Scan(&state, &title, &documentRevision, &fingerprint, &content, &draftRevision); err != nil {
				t.Fatal(err)
			}
			if scenario == "commit" {
				if state != "committed" || title != "updated" || content != "updated" || fingerprint != "new" || documentRevision != 2 || draftRevision != 2 {
					t.Fatalf("unexpected committed graph: %s %s %s %s %d %d", state, title, content, fingerprint, documentRevision, draftRevision)
				}
			} else if state != "pending" || title != "original" || content != "original" || fingerprint != "old" || documentRevision != 1 || draftRevision != 1 {
				t.Fatalf("partial graph escaped rollback: %s %s %s %s %d %d", state, title, content, fingerprint, documentRevision, draftRevision)
			}
		})
	}
}
