package transcribe

import (
	"context"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/sqlx/testutil/sqlfault"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
)

func borrowedAuxiliaryOwnerFixture(t *testing.T) (string, *Source, *Source, *sqlite.Harness) {
	return borrowedAuxiliaryOwnerTableFixture(t, false)
}

func borrowedAuxiliaryOwnerTableFixture(t *testing.T, independentCaller bool) (string, *Source, *Source, *sqlite.Harness) {
	return borrowedAuxiliaryOwnerConfigFixture(t, independentCaller, false, false)
}

func borrowedAuxiliaryOwnerConfigFixture(t *testing.T, independentCaller, foreignInput, physicalOwner bool) (string, *Source, *Source, *sqlite.Harness) {
	t.Helper()
	owner := strings.ReplaceAll(auxiliaryOwnerDQL, "'model','github.com/viant/datly/auxownerfixture/model'", "'self','github.com/viant/datly/auxownerfixture/generated'")
	owner = strings.ReplaceAll(owner, "model.Caller", "self.Caller")
	owner = strings.Replace(owner, "SELECT r.*", "#define($_ = $Callers<?>(view/Callers).Cardinality('Many').TypeName('Caller').WithColumnType('id','int') /* SELECT id,type_id FROM records */)\nSELECT r.*", 1)
	owner = strings.Replace(owner, "SELECT id,type_id,NULL AS note", "SELECT records.*,NULL AS note", 1)
	if independentCaller {
		owner = strings.Replace(owner, "SELECT id,type_id FROM records */)", "SELECT callers.* FROM callers */)", 1)
	}
	if foreignInput {
		owner = strings.Replace(owner, "#setting($_ = $connector", `#import('jwt','github.com/viant/scy/auth/jwt')
#import('auth','github.com/viant/datly/auxownerfixture/support')
#define($_ = $Jwt<string,*jwt.Claims>(header/Authorization).WithCodec('JwtClaim'))
#define($_ = $Auth<*auth.Output>(component/GET:/auth))
#setting($_ = $connector`, 1)
		owner = strings.Replace(owner, "$input_type('Input')", "$input_type('github.com/viant/datly/auxownerfixture/generated.Input')", 1)
		owner = strings.Replace(owner, "$output_type('Output')", "$output_type('github.com/viant/datly/auxownerfixture/generated.Output')", 1)
	}
	if physicalOwner {
		owner = strings.ReplaceAll(owner, "lifecycle_type(r,'BatchLifecycle'),", "")
		owner = strings.ReplaceAll(owner, "FROM (records)", "FROM records")
		owner = strings.Replace(owner, "$Current<[]*Record>", "$Current<[]*CurrentRecord>", 1)
		owner = strings.Replace(owner, "(view/Current).Cardinality('Many').TypeName('Record')", "(view/Current).Cardinality('Many').TypeName('CurrentRecord')", 1)
		start := strings.Index(owner, "#define($_ = $Candidates")
		end := strings.Index(owner[start:], "\n") + start
		owner = owner[:start] + owner[end+1:]
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	(testharness.GeneratedModule{Path: "github.com/viant/datly/auxownerfixture"}).Write(t, root)
	if foreignInput {
		writeSourceFile(t, root, "support/auth.go", "package support\ntype Output struct { Allowed bool }\n")
	}
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER PRIMARY KEY,type_id INTEGER)"))
	if independentCaller {
		require.NoError(t, db.ExecStatements(t.Context(), "CREATE TABLE callers(id INTEGER PRIMARY KEY,type_id INTEGER)"))
	}
	source := &Source{Scope: "github.com/viant/datly/auxownerfixture/source", Connector: "main", Text: owner, Types: typecatalog.NewCatalog(), ColumnRefiner: column.New(column.Connections{"main": db.DB})}
	source.Name = "Owner"
	sourceDir := filepath.Join(root, "source")
	require.NoError(t, os.MkdirAll(sourceDir, 0755))
	source.Path = filepath.Join(sourceDir, "Owner.dql")
	require.NoError(t, os.WriteFile(source.Path, []byte(owner), 0600))
	if foreignInput {
		require.Equal(t, "github.com/viant/scy/auth/jwt", reflect.TypeFor[jwt.Claims]().PkgPath())
		var err error
		source.Types, err = freshBorrowedForeignTypes(t.Context(), source, owner)
		require.NoError(t, err)
	}
	_, err = (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: source, Destination: root})
	require.NoError(t, err)
	if !physicalOwner {
		writeSourceFile(t, root, "generated/lifecycle.go", auxiliaryOwnerHooks)
	}
	_, err = (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: source, Destination: root})
	require.NoError(t, err)
	child := strings.ReplaceAll(owner, "lifecycle_type(r,'BatchLifecycle'),", "")
	child = strings.ReplaceAll(child, "FROM (records)", "FROM records")
	start := strings.Index(child, "#define($_ = $Current")
	end := strings.Index(child, "SELECT r.*")
	child = child[:start] + child[end:]
	child = strings.ReplaceAll(child, "'/records'", "'/internal/write'")
	child = strings.ReplaceAll(child, "$input_type('Input')", "$input_type('ChildInput')")
	child = strings.ReplaceAll(child, "$output_type('Output')", "$output_type('ChildOutput')")
	child = strings.ReplaceAll(child, "$input_type('github.com/viant/datly/auxownerfixture/generated.Input')", "$input_type('github.com/viant/datly/auxownerfixture/generated.ChildInput')")
	child = strings.ReplaceAll(child, "$output_type('github.com/viant/datly/auxownerfixture/generated.Output')", "$output_type('github.com/viant/datly/auxownerfixture/generated.ChildOutput')")
	child = strings.Replace(child, "#define($_ = $Records", `#setting($_ = $file_prefix('child_'))
#setting($_ = $borrow_sql_row('Records','github.com/viant/datly/auxownerfixture/generated','Record','Owner.dql','Owner','Records'))
#define($_ = $Records`, 1)
	childSource := *source
	childSource.Name = "Child"
	childSource.Path = filepath.Join(sourceDir, "Child.dql")
	childSource.Text = child
	childSource.Types = typecatalog.NewCatalog()
	if foreignInput {
		childSource.Types, err = freshBorrowedForeignTypes(t.Context(), &childSource, child)
		require.NoError(t, err)
	}
	require.NoError(t, os.WriteFile(childSource.Path, []byte(child), 0600))
	_, err = (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: &childSource, Destination: root})
	require.NoError(t, err)
	return root, source, &childSource, db
}

func borrowedGeneratedModuleTest(t *testing.T, root string) {
	t.Helper()
	cmd := exec.Command("go", "test", "-mod=mod", "./...")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
}

func TestBorrowedAuxiliaryOwnerNamedClosure(t *testing.T) {
	root, owner, child, db := borrowedAuxiliaryOwnerFixture(t)
	regenerate := func() {
		_, err := (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: owner, Destination: root})
		require.NoError(t, err)
		_, err = (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: child, Destination: root})
		require.NoError(t, err)
	}
	snapshot := func() map[string]string {
		files := map[string]string{}
		require.NoError(t, filepath.WalkDir(filepath.Join(root, "generated"), func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !entry.IsDir() {
				data, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				files[path] = string(data)
			}
			return nil
		}))
		return files
	}
	before := snapshot()
	regenerate()
	require.Equal(t, before, snapshot(), "owner/borrower regeneration changed managed artifacts")
	require.NoError(t, db.ExecStatements(t.Context(), "ALTER TABLE records ADD COLUMN score INTEGER NOT NULL DEFAULT 0"))
	regenerate()
	row, err := os.ReadFile(filepath.Join(root, "generated", "views.go"))
	require.NoError(t, err)
	require.Contains(t, string(row), "Score")
	require.Contains(t, string(row), "score")
	added := snapshot()
	regenerate()
	require.Equal(t, added, snapshot(), "schema-expanded regeneration is unstable")
	require.NoError(t, db.ExecStatements(t.Context(), "DROP TABLE records", "CREATE TABLE records(id INTEGER PRIMARY KEY,type_id INTEGER,score TEXT)"))
	regenerate()
	changed := snapshot()
	require.NotEqual(t, added, changed, "physical type change was not inferred")
	regenerate()
	require.Equal(t, changed, snapshot())
	borrowedGeneratedModuleTest(t, root)
}

func TestBorrowedAuxiliaryOwnerRejectsStaleNamedDependency(t *testing.T) {
	root, owner, child, _ := borrowedAuxiliaryOwnerFixture(t)
	changed := strings.Replace(owner.Text, "SELECT id,type_id FROM records */)", "SELECT id,type_id,id AS extra FROM records */)", 1)
	require.NotEqual(t, owner.Text, changed)
	require.NoError(t, os.WriteFile(owner.Path, []byte(changed), 0600))
	_, err := (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: child, Destination: root})
	require.Error(t, err, "stale named dependency artifact established authority")
	require.Contains(t, err.Error(), "borrow_sql_row")
}

func TestBorrowedAuxiliaryOwnerRejectsTransientMismatch(t *testing.T) {
	root, _, child, _ := borrowedAuxiliaryOwnerFixture(t)
	child.Text = strings.Replace(child.Text, "CAST(r.note AS string)", "CAST(r.note AS *string)", 1)
	require.NoError(t, os.WriteFile(child.Path, []byte(child.Text), 0600))
	_, err := (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: child, Destination: root})
	require.ErrorContains(t, err, "leaf contracts differ")
}

func TestBorrowedPhysicalProjectionRetainsNontransparentClauses(t *testing.T) {
	for _, clause := range []string{"WHERE id>0", "ORDER BY id", "LIMIT 1", "GROUP BY id"} {
		t.Run(clause, func(t *testing.T) {
			result, leaf, declaration := borrowedAuxiliaryNullComparisonFixture(t)
			original := leaf.Source.SQL
			leaf.Source.SQL = "SELECT * FROM (" + original + ") wrapped " + clause
			_, err := borrowedLeafContract(t.Context(), result, leaf, declaration)
			require.ErrorContains(t, err, "physical projection requires", "nontransparent wrapper clause was discarded")
			require.Equal(t, "SELECT * FROM ("+original+") wrapped "+clause, leaf.Source.SQL)
		})
	}
	result, leaf, declaration := borrowedAuxiliaryNullComparisonFixture(t)
	leaf.Source.SQL = "SELECT DISTINCT * FROM (" + leaf.Source.SQL + ") wrapped"
	_, err := borrowedLeafContract(t.Context(), result, leaf, declaration)
	require.ErrorContains(t, err, "physical projection requires")
}

func TestBorrowedAuxiliaryOwnerRejectsInvalidHook(t *testing.T) {
	for _, kind := range []string{"missing", "signature"} {
		t.Run(kind, func(t *testing.T) {
			root, _, child, _ := borrowedAuxiliaryOwnerFixture(t)
			hook := filepath.Join(root, "generated", "lifecycle.go")
			if kind == "missing" {
				require.NoError(t, os.Remove(hook))
			} else {
				data, err := os.ReadFile(hook)
				require.NoError(t, err)
				changed := strings.Replace(string(data), "input *Input", "input *Record", 1)
				require.NotEqual(t, string(data), changed)
				require.NoError(t, os.WriteFile(hook, []byte(changed), 0600))
			}
			_, err := (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: child, Destination: root})
			require.Error(t, err, "unlinked owner lifecycle was accepted")
		})
	}
}

func TestBorrowedAuxiliaryOwnerRejectsNamedHelperDrift(t *testing.T) {
	root, _, child, _ := borrowedAuxiliaryOwnerFixture(t)
	path := filepath.Join(root, "generated", "entities.go")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	source := string(data)
	start := strings.Index(source, "func (entity *Caller) SetId(")
	require.GreaterOrEqual(t, start, 0)
	changed := source[:start] + strings.Replace(source[start:], "entity.Has.Id = true", "entity.Has.Id = false", 1)
	require.NotEqual(t, source, changed)
	require.NoError(t, os.WriteFile(path, []byte(changed), 0600))
	_, err = (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: child, Destination: root})
	require.ErrorContains(t, err, "helper", "stale named setter body established authority")
}

func TestBorrowedAuxiliaryOwnerRejectsStaleNamedTagsAndMarkers(t *testing.T) {
	for _, kind := range []string{"tag", "marker"} {
		t.Run(kind, func(t *testing.T) {
			root, _, child, _ := borrowedAuxiliaryOwnerFixture(t)
			path := filepath.Join(root, "generated", "views.go")
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			source := string(data)
			prefix := "type Caller struct"
			if kind == "marker" {
				prefix = "type CallerHas struct"
			}
			start := strings.Index(source, prefix)
			require.GreaterOrEqual(t, start, 0)
			end := strings.Index(source[start:], "}") + start
			require.Greater(t, end, start)
			body := source[start:end]
			changed := regexp.MustCompile(`json:"id[^"\n]*"`).ReplaceAllString(body, `json:"stale_id"`)
			if kind == "marker" {
				changed = regexp.MustCompile(`Id\s+bool`).ReplaceAllString(body, "Id int")
			}
			require.NotEqual(t, body, changed)
			source = source[:start] + changed + source[end:]
			require.NoError(t, os.WriteFile(path, []byte(source), 0600))
			_, err = (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: child, Destination: root})
			require.ErrorContains(t, err, "native owner metadata/resource is stale")
		})
	}
}

func TestBorrowedAuxiliaryOwnerRevalidatesIndependentNamedSchema(t *testing.T) {
	root, _, child, db := borrowedAuxiliaryOwnerTableFixture(t, true)
	compiled, err := (&Compiler{}).Compile(t.Context(), child)
	require.NoError(t, err)
	var proof *gen.BorrowedRowAuthority
	for _, reference := range compiled.Views {
		if reference != nil && reference.Borrowed != nil {
			proof = reference.Borrowed
		}
	}
	require.NotNil(t, proof)
	require.NoError(t, proof.ValidateSchema())
	before := map[string]string{}
	require.NoError(t, filepath.WalkDir(filepath.Join(root, "generated"), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			before[path] = string(data)
		}
		return nil
	}))
	// Only the dependency table changes; Record's fields and records physical
	// constraints stay identical, so root-only comparison cannot detect drift.
	require.NoError(t, db.ExecStatements(t.Context(), "DROP TABLE callers", "CREATE TABLE callers(id INTEGER PRIMARY KEY,type_id TEXT,extra TEXT)"))
	require.ErrorContains(t, proof.ValidateSchema(), "named dependency contracts differ")
	_, err = (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Compiled: compiled, Destination: root})
	require.ErrorContains(t, err, "named dependency contracts differ")
	for path, want := range before {
		actual, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		require.Equal(t, want, string(actual), "failed closure revalidation published artifacts")
	}
}

func TestBorrowedOwnersRetainForeignInputContracts(t *testing.T) {
	for _, physical := range []bool{false, true} {
		name := "auxiliary"
		if physical {
			name = "physical"
		}
		t.Run(name, func(t *testing.T) {
			root, owner, child, _ := borrowedAuxiliaryOwnerConfigFixture(t, physical, true, physical)
			ownerText, childText := owner.Text, child.Text
			before := borrowedGeneratedSnapshot(t, root)
			_, err := (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: owner, Destination: root})
			require.NoError(t, err)
			_, err = (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: child, Destination: root})
			require.NoError(t, err)
			require.Equal(t, before, borrowedGeneratedSnapshot(t, root))
			require.Equal(t, ownerText, owner.Text)
			require.Equal(t, childText, child.Text)
			for _, file := range []string{"input.go", "child_input.go"} {
				data, err := os.ReadFile(filepath.Join(root, "generated", file))
				require.NoError(t, err)
				require.Regexp(t, `Jwt\s+\*jwt.Claims`, string(data))
				require.Regexp(t, `Auth\s+\*auth.Output`, string(data))
				require.Contains(t, string(data), "dataType=string")
			}
			borrowedGeneratedModuleTest(t, root)
		})
	}
}

func TestBorrowedForeignImportClosureExcludesOwnerDeclarations(t *testing.T) {
	root, owner, _, _ := borrowedAuxiliaryOwnerFixture(t)
	// Even an AST dependency import back to the owning package must not restore
	// stale generated row authority in the fresh source catalog.
	writeSourceFile(t, root, "support/auth.go", `package support
import owned "github.com/viant/datly/auxownerfixture/generated"
type Output struct { Stale *owned.Record }
`)
	text := strings.Replace(owner.Text, "#setting($_ = $connector", `#import('auth','github.com/viant/datly/auxownerfixture/support')
#setting($_ = $connector`, 1)
	fresh, err := freshBorrowedForeignTypes(t.Context(), owner, text)
	require.NoError(t, err)
	_, found, err := fresh.Resolve(typecatalog.TranscribeAuthority, "github.com/viant/datly/auxownerfixture/support.Output")
	require.NoError(t, err)
	require.True(t, found)
	_, found, err = fresh.Resolve(typecatalog.TranscribeAuthority, "github.com/viant/datly/auxownerfixture/generated.Record")
	require.NoError(t, err)
	require.False(t, found, "recursive foreign import restored stale owner row authority")
}

func borrowedGeneratedSnapshot(t *testing.T, root string) map[string]string {
	t.Helper()
	files := map[string]string{}
	require.NoError(t, filepath.WalkDir(filepath.Join(root, "generated"), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			files[path] = string(data)
		}
		return nil
	}))
	return files
}

func TestBorrowedPreparationRetainsOwnerAcrossAdmission(t *testing.T) {
	_, _, child, db := borrowedAuxiliaryOwnerTableFixture(t, true)
	var queries []string
	observed := db.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
		if call.Phase == "query" {
			queries = append(queries, call.SQL)
		}
		return nil
	})
	child.ColumnRefiner = column.New(column.Connections{"main": observed})
	compilation, err := prepareCompilation(t.Context(), child)
	require.NoError(t, err)
	require.NoError(t, compilation.loadMetadata(t.Context(), false))
	require.Len(t, compilation.result.borrowedPreparation.owners, 1)
	var captured *borrowedOwnerPreparation
	for _, entry := range compilation.result.borrowedPreparation.owners {
		captured = entry
	}
	require.NotNil(t, captured.result)
	require.NotNil(t, captured.plan)
	metadataQueries := append([]string(nil), queries...)
	queries = nil
	require.NoError(t, compilation.compileViews(t.Context()))
	require.NoError(t, compilation.refine(t.Context()))
	require.NoError(t, compilation.compileHandler())
	require.NoError(t, compilation.linkTypes())
	require.NoError(t, compilation.authoredInputs.validate(t.Context()))
	queries = nil
	require.NoError(t, (&Compiler{}).admitBorrowedSQLRows(t.Context(), compilation.result))
	require.Same(t, captured, compilation.result.borrowedPreparation.owners[borrowedOwnerKey(child.BaseDir(), compilation.result.AuthoredBorrowedSQLRows[0])])
	countOwnerDiscovery := func(calls []string) int {
		count := 0
		for _, query := range calls {
			if strings.Contains(query, "FROM (records)") && strings.Contains(query, "NULL AS note") {
				count++
			}
		}
		return count
	}
	require.Equal(t, 1, countOwnerDiscovery(metadataQueries), "initial owner must compile exactly once")
	require.Equal(t, 1, countOwnerDiscovery(queries), "admission must reuse initial owner; only the mandatory fresh publication verification rediscovers it")
}

func TestBorrowedPreparationRevalidatesWhileTargetIsStaged(t *testing.T) {
	root, _, child, db := borrowedAuxiliaryOwnerConfigFixture(t, true, true, false)
	compiled, err := (&Compiler{}).Compile(t.Context(), child)
	require.NoError(t, err)
	var proof *gen.BorrowedRowAuthority
	for _, reference := range compiled.Views {
		if reference != nil && reference.Borrowed != nil {
			proof = reference.Borrowed
		}
	}
	require.NotNil(t, proof)
	before := borrowedGeneratedSnapshot(t, root)
	target := filepath.Join(root, "generated")
	staged := filepath.Join(root, "generated.staged")
	require.NoError(t, os.Rename(target, staged))
	support := filepath.Join(root, "support")
	require.NoError(t, os.Rename(support, support+".staged"))
	// Neither destination rediscovery nor reloading Claims/Auth can use the removed package.
	require.NoError(t, proof.ValidateSchema())
	require.NoError(t, db.ExecStatements(t.Context(), "ALTER TABLE callers ADD extra TEXT"))
	require.ErrorContains(t, proof.ValidateSchema(), "named dependency contracts differ")
	require.NoError(t, os.Rename(staged, target))
	require.NoError(t, os.Rename(support+".staged", support))
	require.Equal(t, before, borrowedGeneratedSnapshot(t, root))
	_, err = (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Compiled: compiled, Destination: root})
	require.ErrorContains(t, err, "named dependency contracts differ")
	require.Equal(t, before, borrowedGeneratedSnapshot(t, root))
}

func TestBorrowedCanonicalRowIdentity(t *testing.T) {
	for _, qualified := range []string{"github.com/viant/datly/auxownerfixture/generated.Record", "example.com/foreign.Record"} {
		t.Run(qualified, func(t *testing.T) {
			root, _, child, _ := borrowedAuxiliaryOwnerFixture(t)
			child.Text = strings.Replace(child.Text, "type(r,'Record')", "type(r,'"+qualified+"')", 1)
			require.NoError(t, os.WriteFile(child.Path, []byte(child.Text), 0600))
			_, err := (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: child, Destination: root})
			if strings.HasPrefix(qualified, "example.com/") {
				require.ErrorContains(t, err, "explicit canonical row type differs")
			} else {
				require.NoError(t, err)
				borrowedGeneratedModuleTest(t, root)
			}
		})
	}
}

func TestBorrowedDiscoveryRetainsAuthoredIdentityOnRepeat(t *testing.T) {
	root, _, child, _ := borrowedAuxiliaryOwnerFixture(t)
	discovery := &Discovery{BaseDir: root, Include: []string{"github.com/viant/datly/auxownerfixture/source"}, SourceFile: "Child.dql", ColumnRefiner: child.ColumnRefiner}
	before := borrowedGeneratedSnapshot(t, root)
	for pass := 0; pass < 2; pass++ {
		project, err := discovery.Compile(t.Context())
		require.NoError(t, err)
		require.Len(t, project.Components, 1)
		compiled := project.Components[0]
		require.Equal(t, child.Scope, compiled.Source.Scope)
		require.Equal(t, child.Name, compiled.Source.Name)
		for _, reference := range compiled.Views {
			if reference != nil && reference.Borrowed != nil {
				require.Equal(t, child.Scope, reference.Borrowed.BorrowerScope)
				require.Equal(t, child.Name, reference.Borrowed.BorrowerName)
				require.Contains(t, reference.Borrowed.BorrowerGraph[0], child.Scope)
			}
		}
		_, err = (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
		require.Equal(t, before, borrowedGeneratedSnapshot(t, root))
	}
	borrowedGeneratedModuleTest(t, root)
	// Explicitly selecting the generated package must consume just this holder.
	// The owner is a distinct sibling and remains selected once.
	discovery.SourceFile = ""
	discovery.Include = append(discovery.Include, "github.com/viant/datly/auxownerfixture/generated")
	project, err := discovery.Compile(t.Context())
	require.NoError(t, err)
	names := map[string]int{}
	for _, compiled := range project.Components {
		names[compiled.Component.Name]++
	}
	require.Equal(t, map[string]int{"Owner": 1, "Child": 1}, names)
}
