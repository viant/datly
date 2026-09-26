package transcribe

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/genpatch"
	"github.com/viant/datly/transcribe/column"
)

func TestMutationMarkerPolicyErrors(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, annotation, operation string
		language                    HandlerTarget
	}{
		{"identity flag", "delete_marker(o.ID)", "patch", HandlerGo},
		{"nonboolean flag", "delete_marker(o.NAME)", "patch", HandlerGo},
		{"auxiliary", "concurrency_token(Kinds.ID)", "patch", HandlerGo},
		{"post", "concurrency_token(o.START)", "post", HandlerGo},
		{"get", "delete_marker(o.NAME)", "get", HandlerGo},
		{"velty", "concurrency_token(o.START)", "patch", HandlerVelty},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			testharness.WriteGeneratedGoMod(t, root)
			text := strings.Replace(genpatch.DQL, "SELECT o.*, Items.*, Kinds.*,", "SELECT o.*, Items.*, Kinds.*,"+tc.annotation+",", 1)
			text = strings.Replace(text, "'PATCH'", "'"+strings.ToUpper(tc.operation)+"'", 1)
			_, err := (Generator{Operation: tc.operation, Language: tc.language}).Generate(ctx, GenerationRequest{Destination: root, Source: &Source{Name: "Orders", Text: text, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}})
			if err == nil {
				t.Fatal("invalid policy accepted")
			}
			if _, err = os.Stat(filepath.Join(root, "api", "orders")); !os.IsNotExist(err) {
				t.Fatal("invalid policy published files", err)
			}
		})
	}
}

func TestStringConcurrencyTokenGenerates(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, `CREATE TABLE transitions(run_id TEXT PRIMARY KEY, status TEXT NOT NULL)`,
		`INSERT INTO transitions VALUES('r1','accepted')`); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	const text = `#package('api/transitions')
#setting($_ = $route('/transitions','PATCH'))
SELECT t.run_id, t.status, type(t,'Transition'),
       tag(t.run_id,'sqlx:"run_id,primaryKey"'),
       concurrency_token(t.status), lifecycle_type(t,'TransitionRules')
FROM transitions t`
	if _, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Destination: root,
		Source: &Source{Name: "Transitions", Text: text, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "api", "transitions", "lifecycle.go")); err != nil {
		t.Fatalf("string-token lifecycle scaffold: %v", err)
	}
}
