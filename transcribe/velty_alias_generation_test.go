package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	tcolumn "github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
)

func TestTranscribedVeltyHandlerRetainsAuthoredInputAndColumnSelectors(t *testing.T) {
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx,
		"CREATE TABLE users (id INTEGER NOT NULL)",
		"CREATE TABLE authorization (IS_AUTH BOOLEAN NOT NULL)",
	); err != nil {
		t.Fatalf("create tables: %v", err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	generated, err := transcribeSource(ctx, root, &Source{
		Scope: "example.com/generated/users", Name: "Users", Connector: "main",
		ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": harness.DB}),
		Text: `#setting($_ = $route('/users', 'POST'))
#define($_ = $teamID<string>(query/teamID))
#define($_ = $Authorization<*Authorization>(view/Authorization) /* SELECT IS_AUTH FROM authorization */)
#define($_ = $Result<string>(output/body))
SELECT id FROM users`,
		VeltyHandler: &gen.VeltyHandlerAsset{
			Template: `#if($Input.Authorization.IS_AUTH)#set($Output.Result = $Input.teamID)#end`,
		},
	})
	if err != nil {
		t.Fatalf("Transcribe() error = %v", err)
	}
	if tag := fieldTagByName(generated.Result.Plan.Input.Fields, "TeamID"); !strings.Contains(tag, `velty:"names=TeamID|teamID"`) {
		t.Fatalf("generated input tag = %q", tag)
	}
	var viewTag string
	for _, view := range generated.Result.Plan.Views {
		if view.Name == "Authorization" {
			viewTag = fieldTagByName(view.Fields, "IsAuth")
			break
		}
	}
	if !strings.Contains(viewTag, `velty:"names=IS_AUTH|IsAuth"`) {
		t.Fatalf("generated view tag = %q", viewTag)
	}
	testSource := `package users

import "testing"

func TestGeneratedVeltyAliasesCompile(t *testing.T) {
	if _, err := NewUsersHandler(); err != nil {
		t.Fatal(err)
	}
}
`
	if err = os.WriteFile(filepath.Join(root, "generated", "velty_alias_test.go"), []byte(testSource), 0o644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated Velty alias module failed: %v\n%s", runErr, output)
	}
}

func fieldTagByName(fields []gen.Field, name string) string {
	for _, field := range fields {
		if field.Name == name {
			return field.Tag
		}
	}
	return ""
}
