package generate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func borrowedGuardFixture(t *testing.T) (*BorrowedRowAuthority, string, string) {
	t.Helper()
	root := t.TempDir()
	owner := filepath.Join(root, "owner")
	if err := os.Mkdir(owner, 0755); err != nil {
		t.Fatal(err)
	}
	row := filepath.Join(owner, "row.go")
	marker := filepath.Join(owner, "has.go")
	bodies := map[string]string{row: generatedHeader("Private") + "package owner\ntype Row struct { Id int }\n", marker: generatedHeader("Private") + "package owner\ntype RowHas struct { Id bool }\n", filepath.Join(owner, "helpers.go"): generatedHeader("Private") + "package owner\nfunc (*Row) Helper() {}\n", filepath.Join(root, "Public.dql"): "SELECT r.* FROM main.records r", filepath.Join(root, "embedded.sql"): "SELECT r.* FROM main.records r"}
	proof := &BorrowedRowAuthority{OwnerName: "Private", OwnerDirectory: owner, RowFile: row, HasFile: marker, Expected: BorrowedLeafContract{Package: "example.com/owner", Name: "Row", Fields: []Field{{Name: "Id", Type: "int"}}, MarkerFields: []Field{{Name: "Id", Type: "bool"}}}}
	for path, body := range bodies {
		if err := os.WriteFile(path, []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
		seal, err := SealBorrowedAuthorityFile(path)
		if err != nil {
			t.Fatal(err)
		}
		proof.Files = append(proof.Files, seal)
	}
	return proof, root, owner
}

func TestBorrowedAuthorityUnconditionalContentAndMode(t *testing.T) {
	for _, name := range []string{"row.go", "has.go", "helpers.go", "Public.dql", "embedded.sql"} {
		t.Run(name, func(t *testing.T) {
			proof, root, owner := borrowedGuardFixture(t)
			path := filepath.Join(owner, name)
			if strings.HasSuffix(name, ".sql") || strings.HasSuffix(name, ".dql") {
				path = filepath.Join(root, name)
			}
			if err := proof.ValidateFiles(func(s string) string { return s }); err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			data[len(data)-1] = ' ' // same length, same mtime
			if err = os.WriteFile(path, data, info.Mode()); err != nil {
				t.Fatal(err)
			}
			if err = os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
				t.Fatal(err)
			}
			if err = proof.ValidateFiles(func(s string) string { return s }); err == nil || !strings.Contains(err.Error(), "content drift") {
				t.Fatalf("drift accepted: %v", err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !reflect.DeepEqual(after, data) {
				t.Fatal("latest external edit lost", err)
			}
		})
	}
	t.Run("mode", func(t *testing.T) {
		proof, _, _ := borrowedGuardFixture(t)
		if err := os.Chmod(proof.RowFile, 0600); err != nil {
			t.Fatal(err)
		}
		if err := proof.ValidateFiles(func(s string) string { return s }); err == nil {
			t.Fatal("mode drift accepted")
		}
	})
}

func TestBorrowedAuthorityProtectedSwapRejectsExternalEdit(t *testing.T) {
	proof, root, owner := borrowedGuardFixture(t)
	stage := filepath.Join(root, "stage")
	if err := os.Mkdir(stage, 0755); err != nil {
		t.Fatal(err)
	}
	p := &scaffoldPersistence{borrowed: []*BorrowedRowAuthority{proof}}
	original, stats, err := p.copyExisting(owner, stage)
	if err != nil {
		t.Fatal(err)
	}
	external := filepath.Join(root, "Public.dql")
	data, err := os.ReadFile(external)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(external)
	data[len(data)-1] = 'X'
	if err = os.WriteFile(external, data, 0644); err != nil {
		t.Fatal(err)
	}
	if err = os.Chtimes(external, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	before, err := readScaffoldSnapshot(owner)
	if err != nil {
		t.Fatal(err)
	}
	if err = p.swap(owner, stage, original, stats); err == nil || !strings.Contains(err.Error(), "content drift") {
		t.Fatalf("swap accepted external drift: %v", err)
	}
	after, err := readScaffoldSnapshot(owner)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("protected owner changed", err)
	}
	got, err := os.ReadFile(external)
	if err != nil || !reflect.DeepEqual(got, data) {
		t.Fatal("latest external edit lost", err)
	}
}

func TestBorrowedLeafComparisonOnlyHas(t *testing.T) {
	view := &spec.View{Name: "Records", TypeName: "Row", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "ID", Source: "ID", Type: spec.TypeRef{Name: "int"}, PrimaryKey: true}}}
	component := &spec.Component{Name: "Private", RootView: view, Settings: &spec.Settings{Mutation: "patch", DefaultConnector: "main"}}
	expected, err := BorrowedLeafContractFor(component, view, "example.com/owner", "Row", "main", "", "main", "records")
	if err != nil {
		t.Fatal(err)
	}
	if len(expected.MarkerFields) != 1 || expected.MarkerFields[0].Name != "Id" {
		t.Fatalf("native Has membership: %+v", expected.MarkerFields)
	}
	aux := component.Clone()
	aux.RootView.Auxiliary = true
	actual, err := BorrowedLeafContractFor(aux, aux.RootView, "example.com/owner", "Row", "main", "", "main", "records")
	if err != nil {
		t.Fatal(err)
	}
	if err = CompareBorrowedLeafContracts(expected, actual); err != nil {
		t.Fatal(err)
	}
	if !aux.RootView.Auxiliary || component.RootView.Auxiliary {
		t.Fatal("comparison mutated semantic role")
	}
	for _, difference := range []string{"nullable", "default", "type", "fk", "marker"} {
		t.Run(difference, func(t *testing.T) {
			changed := actual
			changed.Columns = []*spec.Column{actual.Columns[0].Clone()}
			switch difference {
			case "nullable":
				changed.Columns[0].Nullable = true
			case "default":
				value := "0"
				changed.Columns[0].Default = &value
			case "type":
				changed.Columns[0].Type.Name = "string"
			case "fk":
				changed.Columns[0].Tag = "sqlx:\"ID,refTable=other\""
			case "marker":
				changed.MarkerFields = nil
			}
			if CompareBorrowedLeafContracts(expected, changed) == nil {
				t.Fatal("contract difference accepted")
			}
		})
	}
	unknown := component.Clone()
	unknown.RootView.Source.Table = "different"
	if _, err = BorrowedLeafContractFor(unknown, unknown.RootView, "example.com/owner", "Row", "main", "", "main", "records"); err == nil {
		t.Fatal("unknown schema accepted")
	}
}

func TestBorrowedAdditionalMetadataAndReceiptOwnership(t *testing.T) {
	view := &spec.View{Name: "Rows", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "ID", Source: "ID", Type: spec.TypeRef{Name: "int"}, PrimaryKey: true}}}
	component := &spec.Component{Name: "Public", RootView: view, Settings: &spec.Settings{Mutation: "patch"}}
	expected, err := BorrowedLeafContractFor(component, view, "example.com/api", "Row", "main", "", "main", "records")
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{"inMemory", "allowNulls", "cardinality", "hooks", "control", "binding", "nullPolicy", "actionPolicy"} {
		t.Run(change, func(t *testing.T) {
			changed := component.Clone()
			v := changed.RootView
			switch change {
			case "inMemory":
				v.InMemory = true
			case "allowNulls":
				value := true
				v.AllowNulls = &value
			case "cardinality":
				v.Cardinality = spec.CardinalityOne
			case "hooks":
				v.EntityHooks = "example.com/api.Hooks"
			case "control":
				v.Source.Controls = &spec.ViewControls{OrderBy: "id"}
			case "binding":
				v.Source.Bindings = &spec.ViewBindings{CacheName: "cache"}
			case "nullPolicy":
				v.RootNullPolicy = "reject"
			case "actionPolicy":
				v.WriterActionPolicy = "insert-delete"
			}
			actual, err := BorrowedLeafContractFor(changed, v, "example.com/api", "Row", "main", "", "main", "records")
			if err == nil && CompareBorrowedLeafContracts(expected, actual) == nil {
				t.Fatal("view metadata difference accepted")
			}
		})
	}
	root := t.TempDir()
	proof := &BorrowedRowAuthority{BorrowerSource: "/source/Public.dql", BorrowerScope: "source", BorrowerName: "Public"}
	plan := &Plan{ComponentName: "Public", BorrowedRows: []*BorrowedRowAuthority{proof}}
	files, err := borrowedReceiptFiles(root, plan)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(files[0].Path, []byte(files[0].Content), 0600); err != nil {
		t.Fatal(err)
	}
	proof.ReceiptGuard = nil // Fresh admission for a later generation.
	if _, err = borrowedReceiptFiles(root, plan); err != nil {
		t.Fatal("same owner repeat failed", err)
	}
	other := proof.Clone()
	other.BorrowerScope = "other"
	other.ReceiptGuard = nil
	plan.BorrowedRows = []*BorrowedRowAuthority{other}
	if _, err = borrowedReceiptFiles(root, plan); err == nil {
		t.Fatal("foreign receipt overwritten")
	}
	plan.BorrowedRows[0].ReceiptGuard = nil
	if err = os.WriteFile(files[0].Path, []byte("authored foreign content"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = borrowedReceiptFiles(root, plan); err == nil {
		t.Fatal("authored receipt overwritten")
	}
}

func TestBorrowedProjectedOwnerAndBoundarySchemaDrift(t *testing.T) {
	proof, root, owner := borrowedGuardFixture(t)
	stage := filepath.Join(root, "stage")
	if err := os.Mkdir(stage, 0755); err != nil {
		t.Fatal(err)
	}
	p := &scaffoldPersistence{}
	if _, _, err := p.copyExisting(owner, stage); err != nil {
		t.Fatal(err)
	}
	set := &packageSet{plans: []*Plan{{BorrowedRows: []*BorrowedRowAuthority{proof}}}, dirs: []string{owner}, forests: []*scaffoldForest{{target: owner, stage: stage}}}
	if err := set.validateBorrowedRows(); err != nil {
		t.Fatal(err)
	}
	row := borrowedIdentityPath(proof.RowFile, owner, stage)
	data, err := os.ReadFile(row)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(row, []byte(strings.Replace(string(data), "Id int", "Id string", 1)), 0644); err != nil {
		t.Fatal(err)
	}
	if err = set.validateBorrowedRows(); err == nil {
		t.Fatal("superseded live owner satisfied projected replacement")
	}
	if err = proof.ValidateFiles(func(s string) string { return s }); err != nil {
		t.Fatal("live owner drifted", err)
	}
	if err = os.WriteFile(row, data, 0644); err != nil {
		t.Fatal(err)
	}
	schemaDrift := false
	reads := 0
	proof.ValidateSchema = func() error {
		reads++
		if schemaDrift {
			return fmt.Errorf("physical schema identity drift")
		}
		return nil
	}
	if err = set.validateBorrowedRows(); err != nil || reads != 1 {
		t.Fatal("prepared schema validation missing", err)
	}
	schemaDrift = true
	if err = set.validateBorrowedRows(); err == nil {
		t.Fatal("prepared schema identity drift accepted")
	}
	protected := &scaffoldPersistence{borrowed: []*BorrowedRowAuthority{proof}}
	original, stats, err := protected.copyExisting(owner, stage)
	if err != nil {
		t.Fatal(err)
	}
	before, err := readScaffoldSnapshot(owner)
	if err != nil {
		t.Fatal(err)
	}
	if err = protected.swap(owner, stage, original, stats); err == nil || !strings.Contains(err.Error(), "schema identity drift") {
		t.Fatalf("protected schema drift accepted: %v", err)
	}
	after, err := readScaffoldSnapshot(owner)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("schema rejection changed owner", err)
	}
}

func TestBorrowedOmissionPolicyAndValidatedSealRetention(t *testing.T) {
	v := &spec.View{Name: "Rows", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "ID", Source: "ID", Type: spec.TypeRef{Name: "int"}, PrimaryKey: true}}}
	c := &spec.Component{Name: "Private", RootView: v, Settings: &spec.Settings{Mutation: "patch"}}
	owner, err := BorrowedLeafContractFor(c, v, "example.com/api", "Row", "main", "", "main", "records")
	if err != nil {
		t.Fatal(err)
	}
	borrower := c.Clone()
	borrower.Settings.Generation = &spec.GenerationSettings{WriterOmitEmpty: true}
	changed, err := BorrowedLeafContractFor(borrower, borrower.RootView, "example.com/api", "Row", "main", "", "main", "records")
	if err != nil {
		t.Fatal(err)
	}
	if CompareBorrowedLeafContracts(owner, changed) == nil {
		t.Fatal("writer omission mismatch was erased")
	}
	same := borrower.Clone()
	match, err := BorrowedLeafContractFor(same, same.RootView, "example.com/api", "Row", "main", "", "main", "records")
	if err != nil || CompareBorrowedLeafContracts(changed, match) != nil {
		t.Fatal("matching omission policy rejected", err)
	}
	root := t.TempDir()
	path := filepath.Join(root, "resource.sql")
	if err = os.WriteFile(path, []byte("SELECT 1"), 0600); err != nil {
		t.Fatal(err)
	}
	validated, err := SealBorrowedAuthorityFile(path)
	if err != nil {
		t.Fatal(err)
	}
	proof := &BorrowedRowAuthority{}
	if err = RetainBorrowedSeal(proof, validated); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, []byte("SELECT 2"), 0600); err != nil {
		t.Fatal(err)
	}
	later, err := SealBorrowedAuthorityFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = RetainBorrowedSeal(proof, later); err == nil {
		t.Fatal("changed artifact baseline adopted")
	}
	if string(proof.Files[0].Bytes) != "SELECT 1" {
		t.Fatal("validated evidence replaced")
	}
}

func TestBorrowedReceiptPublicationWindows(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprint(existing), func(t *testing.T) {
			proof, root, owner := borrowedGuardFixture(t)
			proof.BorrowerSource = "/source/Public.dql"
			proof.BorrowerScope = "source"
			proof.BorrowerName = "Public"
			plan := &Plan{BorrowedRows: []*BorrowedRowAuthority{proof}}
			path := filepath.Join(owner, "borrowed_authority.json")
			if existing {
				data, err := json.Marshal(plan.BorrowedRows)
				if err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(path, data, 0644); err != nil {
					t.Fatal(err)
				}
			}
			desired, err := borrowedReceiptFiles(owner, plan)
			if err != nil {
				t.Fatal(err)
			}
			stage := filepath.Join(root, "stage")
			if err = os.Mkdir(stage, 0755); err != nil {
				t.Fatal(err)
			}
			// Competing content after rendering and before copyExisting must not become
			// a new ownership baseline. Same-size/matching-mtime is also exercised.
			var latest []byte
			if existing {
				latest = append([]byte(nil), proof.ReceiptGuard.Prior.Bytes...)
				latest[len(latest)-1] ^= 1
				info, _ := os.Stat(path)
				if err = os.WriteFile(path, latest, 0644); err != nil {
					t.Fatal(err)
				}
				if err = os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
					t.Fatal(err)
				}
			} else {
				latest = []byte("foreign receipt introduced after render")
				if err = os.WriteFile(path, latest, 0644); err != nil {
					t.Fatal(err)
				}
			}
			persistence := &scaffoldPersistence{borrowed: []*BorrowedRowAuthority{proof}, files: desired}
			original, stats, err := persistence.copyExisting(owner, stage)
			if err != nil {
				t.Fatal(err)
			}
			if err = persistence.writeFiles(owner, stage); err != nil {
				t.Fatal(err)
			}
			if err = proof.ValidateProjectedFiles(func(s string) string { return borrowedIdentityPath(s, owner, stage) }); err == nil {
				t.Fatal("prepared competing receipt accepted")
			}
			if err = persistence.swap(owner, stage, original, stats); err == nil {
				t.Fatal("protected competing receipt overwritten")
			}
			got, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(got, latest) {
				t.Fatal("latest receipt edit lost", err)
			}
		})
	}
}

func TestBorrowedReceiptRejectsChangedAuthorityAfterRendering(t *testing.T) {
	proof, root, _ := borrowedGuardFixture(t)
	proof.BorrowerSource, proof.BorrowerScope, proof.BorrowerName = "Public.dql", "Public", "Public"
	plan := &Plan{BorrowedRows: []*BorrowedRowAuthority{proof}}
	if _, err := borrowedReceiptFiles(root, plan); err != nil {
		t.Fatal(err)
	}
	proof.BorrowerScope = "Other"
	if _, err := borrowedReceiptFiles(root, plan); err == nil || !strings.Contains(err.Error(), "authority changed") {
		t.Fatalf("stale prepared receipt accepted for changed authority: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "borrowed_authority.json")); !os.IsNotExist(err) {
		t.Fatal("receipt rendering published content", err)
	}
}

func TestBorrowedNativeIndexDriftAtPublicationBoundaries(t *testing.T) {
	for _, change := range []struct {
		name string
		sql  []string
	}{{"nonunique", []string{"DROP INDEX pair_unique", "CREATE INDEX pair_unique ON records(a,b)"}}, {"reordered", []string{"DROP INDEX pair_unique", "CREATE UNIQUE INDEX pair_unique ON records(b,a)"}}, {"removed", []string{"DROP INDEX pair_unique"}}} {
		t.Run(change.name, func(t *testing.T) {
			ctx := context.Background()
			proof, root, owner := borrowedGuardFixture(t)
			dbPath := filepath.Join(root, "exclusive-publication.db")
			db := sqlite.New(t, sqlite.WithDSN(dbPath))
			t.Logf("EXCLUSIVE_SQLITE owner=%s path=%s", t.Name(), dbPath)
			require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,a INTEGER,b INTEGER)", "CREATE UNIQUE INDEX pair_unique ON records(a,b)"))
			reads := 0
			read := func() ([]byte, error) {
				reads++
				product, err := metadata.New().DetectProduct(ctx, db.DB)
				if err != nil {
					return nil, err
				}
				var indexes []sink.Index
				if err = metadata.New().Info(ctx, db.DB, info.KindIndexes, &indexes, product, option.NewArgs("", "main", "records")); err != nil {
					return nil, err
				}
				type fact struct {
					Index   sink.Index
					Members []sink.Column
				}
				facts := make([]fact, 0, len(indexes))
				for _, idx := range indexes {
					var members []sink.Column
					if err = metadata.New().Info(ctx, db.DB, info.KindIndex, &members, product, option.NewArgs("", "main", "records", idx.Name)); err != nil {
						return nil, err
					}
					facts = append(facts, fact{idx, members})
				}
				return json.Marshal(facts)
			}
			baseline, err := read()
			require.NoError(t, err)
			proof.Expected.Physical = append([]byte(nil), baseline...)
			proof.ValidateSchema = func() error {
				fresh, err := read()
				if err != nil {
					return err
				}
				if !bytes.Equal(baseline, fresh) {
					return fmt.Errorf("fresh native index contract drift")
				}
				return nil
			}
			stage := filepath.Join(root, "stage")
			require.NoError(t, os.Mkdir(stage, 0755))
			protected := &scaffoldPersistence{borrowed: []*BorrowedRowAuthority{proof}}
			original, stats, err := protected.copyExisting(owner, stage)
			require.NoError(t, err)
			set := &packageSet{plans: []*Plan{{BorrowedRows: []*BorrowedRowAuthority{proof}}}, dirs: []string{owner}, forests: []*scaffoldForest{{target: owner, stage: stage}}}
			require.NoError(t, set.validateBorrowedRows())
			require.Equal(t, 2, reads)
			require.NoError(t, db.ExecStatements(ctx, change.sql...))
			require.ErrorContains(t, set.validateBorrowedRows(), "fresh native index contract drift")
			require.Equal(t, 3, reads)
			before, err := readScaffoldSnapshot(owner)
			require.NoError(t, err)
			require.ErrorContains(t, protected.swap(owner, stage, original, stats), "fresh native index contract drift")
			require.GreaterOrEqual(t, reads, 4)
			after, err := readScaffoldSnapshot(owner)
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.Equal(t, baseline, []byte(proof.Expected.Physical))
		})
	}
}

// This exercises the gap after projected validation and before protected swap.
// Each file is mutated only after the earlier check passed; publication must
// reread the prepared bytes and mode while preserving original/external bytes.
func TestBorrowedPreparedPublicationBoundary(t *testing.T) {
	for _, priorReceipt := range []bool{false, true} {
		for _, name := range []string{"row.go", "has.go", "helpers.go", "embedded.sql", "borrowed_authority.json"} {
			for _, change := range []string{"content", "mode", "removed", "symlink"} {
				t.Run(fmt.Sprintf("prior=%v/%s/%s", priorReceipt, name, change), func(t *testing.T) {
					proof, root, owner := borrowedGuardFixture(t)
					resource := filepath.Join(owner, "embedded.sql")
					require.NoError(t, os.WriteFile(resource, []byte("SELECT r.* FROM main.records r"), 0644))
					seal, err := SealBorrowedAuthorityFile(resource)
					require.NoError(t, err)
					proof.Files = append(proof.Files, seal)
					proof.BorrowerSource, proof.BorrowerScope, proof.BorrowerName = "/source/Public.dql", "source", "Public"
					plan := &Plan{BorrowedRows: []*BorrowedRowAuthority{proof}}
					if priorReceipt {
						data, err := json.Marshal(plan.BorrowedRows)
						require.NoError(t, err)
						require.NoError(t, os.WriteFile(filepath.Join(owner, "borrowed_authority.json"), data, 0644))
					}
					desired, err := borrowedReceiptFiles(owner, plan)
					require.NoError(t, err)
					stage := filepath.Join(root, "stage")
					require.NoError(t, os.Mkdir(stage, 0755))
					persistence := &scaffoldPersistence{borrowed: []*BorrowedRowAuthority{proof}, files: desired}
					original, stats, err := persistence.copyExisting(owner, stage)
					require.NoError(t, err)
					require.NoError(t, persistence.writeFiles(owner, stage))
					require.NoError(t, proof.ValidateProjectedFiles(func(path string) string { return borrowedIdentityPath(path, owner, stage) }))
					before, err := readScaffoldSnapshot(owner)
					require.NoError(t, err)
					external := filepath.Join(root, "unrelated-external-edit.txt")
					latest := []byte("external edit after projected validation")
					require.NoError(t, os.WriteFile(external, latest, 0600))
					changed := filepath.Join(stage, name)
					switch change {
					case "content":
						data, err := os.ReadFile(changed)
						require.NoError(t, err)
						info, err := os.Stat(changed)
						require.NoError(t, err)
						data[len(data)-1] ^= 1
						require.NoError(t, os.WriteFile(changed, data, info.Mode()))
						require.NoError(t, os.Chtimes(changed, info.ModTime(), info.ModTime()))
					case "mode":
						require.NoError(t, os.Chmod(changed, 0600))
					case "removed":
						require.NoError(t, os.Remove(changed))
					case "symlink":
						require.NoError(t, os.Remove(changed))
						require.NoError(t, os.Symlink(filepath.Join(owner, name), changed))
					}
					require.Error(t, persistence.swap(owner, stage, original, stats), "prepared drift published after projected validation")
					after, err := readScaffoldSnapshot(owner)
					require.NoError(t, err)
					require.Equal(t, before, after, "protected original changed")
					got, err := os.ReadFile(external)
					require.NoError(t, err)
					require.Equal(t, latest, got, "external edit lost")
					require.DirExists(t, stage, "stage was consumed despite rejection")
				})
			}
		}
	}
}

func TestBorrowedPreparedPublicationBoundaryUnchanged(t *testing.T) {
	for _, priorReceipt := range []bool{false, true} {
		t.Run(fmt.Sprint(priorReceipt), func(t *testing.T) {
			proof, root, owner := borrowedGuardFixture(t)
			proof.BorrowerSource, proof.BorrowerScope, proof.BorrowerName = "/source/Public.dql", "source", "Public"
			plan := &Plan{BorrowedRows: []*BorrowedRowAuthority{proof}}
			if priorReceipt {
				data, err := json.Marshal(plan.BorrowedRows)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filepath.Join(owner, "borrowed_authority.json"), data, 0644))
			}
			desired, err := borrowedReceiptFiles(owner, plan)
			require.NoError(t, err)
			stage := filepath.Join(root, "stage")
			require.NoError(t, os.Mkdir(stage, 0755))
			persistence := &scaffoldPersistence{borrowed: []*BorrowedRowAuthority{proof}, files: desired}
			original, stats, err := persistence.copyExisting(owner, stage)
			require.NoError(t, err)
			require.NoError(t, persistence.writeFiles(owner, stage))
			require.NoError(t, proof.ValidateProjectedFiles(func(path string) string { return borrowedIdentityPath(path, owner, stage) }))
			require.NoError(t, persistence.swap(owner, stage, original, stats))
			published, err := os.ReadFile(filepath.Join(owner, "borrowed_authority.json"))
			require.NoError(t, err)
			require.Equal(t, proof.ReceiptGuard.Expected, published)
		})
	}
}
