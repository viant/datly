package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe/column"
)

func TestPreparedRelationSharesGeneratedRequestLeaf(t *testing.T) {
	for _, tc := range []struct {
		description            string
		qualified, publicFirst bool
	}{
		{description: "local"}, {description: "qualified", qualified: true}, {description: "public first", publicFirst: true},
	} {
		t.Run(tc.description, func(t *testing.T) {
			testPreparedRelation(t, tc.qualified, tc.publicFirst)
		})
	}
}

func testPreparedRelation(t *testing.T, qualified, publicFirst bool) {
	db := sqlite.New(t)
	ctx := context.Background()
	require.NoError(t, db.ExecStatements(ctx,
		"CREATE TABLE owners(id INTEGER PRIMARY KEY)",
		"CREATE TABLE members(id INTEGER PRIMARY KEY,owner_id INTEGER NOT NULL)"))
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/prepared"}).Write(t, root)
	source := &Source{Name: "Owners", Scope: "example.com/prepared/dql", Connector: "main",
		ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: `
#package('example.com/prepared/generated')
#import('self','example.com/prepared/generated')
#setting($_ = $route('/owners','PATCH'))
#define($_ = $Owners<[]*Owner>(body/data))
#define($_ = $Data<[]*Owner>(output/body))
#define($_ = $CurrentOwners<[]*Owner>(view/CurrentOwners).Cardinality('Many') /* SELECT current_rows.*,cast(current_rows.id AS int) FROM (SELECT * FROM owners) current_rows */)
#define($_ = $CurrentMembers<[]*Member>(view/CurrentMembers).Cardinality('Many') /* SELECT current_rows.*,cast(current_rows.id AS int),cast(current_rows.owner_id AS int) FROM (SELECT * FROM members) current_rows */)
SELECT o.*, type(o,'Owner'), cast(o.id AS int),
 w.*, type(w,'Member','PreparedMembers'), tag(w,'json:"-" internal:"true" mcp:"-"'),
 required(w.id), required(w.owner_id),
 internal_transient(w.remove), cast(w.remove AS bool), delete_marker(w.remove), writer_action_policy(w,'insert-delete'),
 p.*, type(p,'Member','Members'), required(p.id), required(p.owner_id), internal_transient(p.remove), cast(p.remove AS bool), tag(p.remove,'writer:"delete"')
FROM (SELECT id FROM (owners)) o
LEFT JOIN (SELECT *,false AS remove FROM members) w ON o.id=w.owner_id
LEFT JOIN (SELECT *,false AS remove FROM (members)) p ON o.id=p.owner_id`}
	if qualified {
		source.Text = strings.ReplaceAll(source.Text, "'Member'", "'self.Member'")
		source.Text = strings.ReplaceAll(source.Text, "[]*Member", "[]*self.Member")
	}
	if publicFirst {
		source.Text = strings.Replace(source.Text, "$CurrentMembers<[]*Member>", "$CurrentMembers<[]*CurrentMember>", 1)
		physical := "LEFT JOIN (SELECT *,false AS remove FROM members) w ON o.id=w.owner_id"
		public := "LEFT JOIN (SELECT *,false AS remove FROM (members)) p ON o.id=p.owner_id"
		source.Text = strings.Replace(source.Text, physical+"\n"+public, public+"\n"+physical, 1)
	}
	generate := func() *GeneratedPackage {
		compiled, err := (&Discovery{BaseDir: root}).CompileSource(ctx, source)
		require.NoError(t, err)
		generated, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
		return generated
	}
	generated := generate()
	var code strings.Builder
	for _, file := range generated.Result.Files {
		code.WriteString(file.Content)
	}
	require.Equal(t, 1, strings.Count(code.String(), "type Member struct"))
	require.Equal(t, 1, strings.Count(code.String(), "type MemberHas struct"))
	require.Contains(t, code.String(), "PreparedMembers")
	require.Equal(t, generated.Result.Files, generate().Result.Files)
	require.Len(t, generated.Result.Plan.ViewByType("Member").RoleIdentities, 1)
	for _, tc := range []struct{ description, annotation string }{
		{"different type", "cast(p.owner_id AS string)"},
		{"different wire tag", `tag(p.owner_id,'json:"other"')`},
		{"different mapping", `tag(p.owner_id,'sqlx:"another_column"')`},
	} {
		t.Run(tc.description, func(t *testing.T) {
			bad := *source
			bad.Text = strings.Replace(bad.Text, "SELECT o.*", "SELECT "+tc.annotation+", o.*", 1)
			compiled, err := (&Discovery{BaseDir: root}).CompileSource(ctx, &bad)
			if err == nil {
				_, err = (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Compiled: compiled, Destination: root})
			}
			require.Error(t, err)
		})
	}
	runtimeSource, err := os.ReadFile("testdata/prepared_relation/runtime_test.go.txt")
	require.NoError(t, err)
	if publicFirst {
		runtimeSource = []byte(strings.Replace(string(runtimeSource), "row := new(Member)", "row := new(CurrentMember)", 1))
	}
	require.NoError(t, os.WriteFile(filepath.Join(root, "generated", "prepared_test.go"), runtimeSource, 0600))
	command := exec.CommandContext(ctx, "go", "test", "-mod=mod", "-count=1", "./...")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s", output)
	require.NoError(t, db.ExecStatements(ctx, "ALTER TABLE members ADD COLUMN label TEXT"))
	updated := generate()
	_, found := updated.Result.Plan.ViewByType("Member").Field("Label")
	require.True(t, found)
	require.Contains(t, updated.Result.Plan.ViewByType("Member").SetMarkerFields, "Label")
}
