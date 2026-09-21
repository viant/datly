package transcribe

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
)

// Original Datly repository/shape is the emission baseline (impl/149,152).
// Compile the emitted ABI once; reload persisted metadata and SQL through the
// same bootstrap, report and application owners used by application packages.
func TestGeneratedCubeComposePublicReloadSQLite(t *testing.T) {
	testGeneratedCubeComposePublicReloadSQLite(t, false)
}

func TestGeneratedCubeExplicitAliasReloadSQLite(t *testing.T) {
	testGeneratedCubeComposePublicReloadSQLite(t, true)
}

func testGeneratedCubeComposePublicReloadSQLite(t *testing.T, explicitAlias bool) {
	ctx := context.Background()
	const module = "github.com/viant/datly/testfixture/composereload"
	authored, root := t.TempDir(), t.TempDir()
	(testharness.GeneratedModule{Path: module}).Write(t, authored)
	if err := os.MkdirAll(filepath.Join(authored, "spend"), 0755); err != nil {
		t.Fatal(err)
	}
	var stages []string
	var contracts map[string]string
	for revision := 1; revision <= 2; revision++ {
		stage := filepath.Join(root, fmt.Sprint(revision))
		if err := os.MkdirAll(stage, 0755); err != nil {
			t.Fatal(err)
		}
		if revision == 1 {
			(testharness.GeneratedModule{Path: module}).Write(t, stage)
		} else if err := os.CopyFS(stage, os.DirFS(stages[0])); err != nil {
			t.Fatal(err)
		}
		sourceBytes, err := os.ReadFile("testdata/compose_reload/Spend.sql")
		if err != nil {
			t.Fatal(err)
		}
		source := fmt.Sprintf(string(sourceBytes), module, revision)
		if explicitAlias {
			source = strings.Replace(source, "s.account_id, SUM(s.amount) AS total_spend", "s.account_id AS AccountId, SUM(s.amount) AS TotalSpend", 1)
			source = strings.ReplaceAll(source, "CAST(s.account_id", "CAST(s.AccountId")
			source = strings.ReplaceAll(source, "tag(s.account_id", "tag(s.AccountId")
			source = strings.ReplaceAll(source, "CAST(s.total_spend", "CAST(s.TotalSpend")
		}
		if err := os.WriteFile(filepath.Join(authored, "spend", "Spend.sql"), []byte(source), 0644); err != nil {
			t.Fatal(err)
		}
		project, err := (&Discovery{BaseDir: authored, Include: []string{module + "/spend"}, Connector: "main"}).Compile(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(project.Components) != 1 {
			t.Fatalf("authored components=%d", len(project.Components))
		}
		compiled := project.Components[0]
		if compiled.Component.Settings.Report.Compose == nil || !compiled.Component.Settings.Report.Compose.Enabled {
			t.Fatal("DQL compose metadata missing")
		}
		if len(compiled.Component.RootView.Columns) != 2 {
			t.Fatalf("authored canonical columns=%+v", compiled.Component.RootView.Columns)
		}
		input, dir, err := generationInput(stage, "spend", compiled)
		if err != nil {
			t.Fatal(err)
		}
		input.SQLResources = true
		generated, err := NewCompiler().generateInputAt(ctx, stage, dir, compiled, input)
		if err != nil {
			t.Fatal(err)
		}
		dir = filepath.Join(stage, dir)
		plan := generated.Result.Plan
		got := map[string]string{}
		for _, name := range []string{plan.Input.Destination, plan.Output.Destination, plan.ViewDest, plan.RouterDest} {
			data, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			got[name] = string(data)
		}
		if revision == 1 {
			contracts = got
		} else if !reflect.DeepEqual(contracts, got) {
			t.Fatalf("compiled Go contracts/holder changed: before=%v after=%v", contracts, got)
		}
		for _, name := range []string{"predicate.go", "reload_test.go", "public_test.go", "deadline_test.go"} {
			data, err := os.ReadFile("testdata/compose_reload/" + name + ".txt")
			if err != nil {
				t.Fatal(err)
			}
			if explicitAlias && name == "public_test.go" {
				text := string(data)
				for _, pair := range [][2]string{{"t1.account_id", "t1.AccountId"}, {"t2.account_id", "t2.AccountId"}, {"t1.total_spend", "t1.TotalSpend"}, {"t2.total_spend", "t2.TotalSpend"}, {`dimensionOutput = "account_id"`, `dimensionOutput = "AccountId"`}, {`measureOutput = "total_spend"`, `measureOutput = "TotalSpend"`}, {`rejectedDimension = "AccountId"`, `rejectedDimension = "account_id"`}, {`rejectedMeasure = "TotalSpend"`, `rejectedMeasure = "total_spend"`}} {
					text = strings.ReplaceAll(text, pair[0], pair[1])
				}
				data = []byte(text)
			}
			if err = os.WriteFile(filepath.Join(dir, name), data, 0644); err != nil {
				t.Fatal(err)
			}
		}
		stages = append(stages, stage)
	}
	command := exec.Command("go", "test", "-mod=mod", "-count=1", "-timeout=180s", "-v", "-run", "^TestComposeReload$", "./spend")
	command.Dir = stages[0]
	command.Env = append(os.Environ(), "GOWORK=off", "DATLY_COMPOSE_STAGE="+stages[1])
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("generated compose acceptance: %v\n%s", err, output)
	}
	for _, protocol := range []string{"http", "mcp-registry", "mcp-session", "mcp-stateless"} {
		if !strings.Contains(string(output), "--- PASS: TestComposeReload/"+protocol) {
			t.Fatalf("consumer did not execute %s: %s", protocol, output)
		}
	}
	t.Logf("compiled generated consumer:\n%s", output)
}
