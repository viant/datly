package config_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/viant/datly/standalone/config"
)

func TestInstanceConstantFilePaths(t *testing.T) {
	root := t.TempDir()
	for name, body := range map[string]string{"e2e.yaml": "Stage: e2e\n", "prod.json": `{"Stage":"prod"}`, "e2e.json": `{"Connectors":[{"Name":"db","Driver":"sqlite3","DSN":"${Stage}.db"}]}`} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	text := `{"ConstURL":"e2e.yaml","BaseDir":".","DependencyURL":"${Stage}.json","ContentURL":"https://example.test/${Stage}/assets"}`
	file := filepath.Join(root, "config.json")
	if err := os.WriteFile(file, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	authored, err := (config.Loader{}).Load(context.Background(), file)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(authored)
	access, err := authored.ResolveConstants()
	if err != nil {
		t.Fatal(err)
	}
	if access.ContentURL != "https://example.test/e2e/assets" || access.Connectors[0].DSN != "e2e.db" {
		t.Fatalf("unexpected access config: %+v", access)
	}
	after, _ := json.Marshal(authored)
	if string(before) != string(after) {
		t.Fatal("authored config mutated")
	}
	if authored.ContentURL != "https://example.test/${Stage}/assets" || authored.Connectors[0].DSN != "${Stage}.db" {
		t.Fatal("configuration lost placeholders")
	}
	bytes, _ := os.ReadFile(file)
	if string(bytes) != text {
		t.Fatal("configuration file changed")
	}
}

func TestInstancePathsExpandBeforeURLJoining(t *testing.T) {
	root := t.TempDir()
	constants, _ := json.Marshal(map[string]string{"Module": root, "Assets": "https://assets.example.test/e2e/", "Dependency": filepath.Join(root, "dependency.json")})
	for name, data := range map[string][]byte{"instance.json": constants, "dependency.json": []byte(`{"Connectors":[]}`), "config.json": []byte(`{"ConstURL":"instance.json","BaseDir":"${Module}","ContentURL":"${Assets}","DependencyURL":"${Dependency}"}`)} {
		if err := os.WriteFile(filepath.Join(root, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	authored, err := (config.Loader{}).Load(context.Background(), filepath.Join(root, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	access, err := authored.ResolveConstants()
	if err != nil {
		t.Fatal(err)
	}
	if access.BaseDir != root || access.ContentURL != "https://assets.example.test/e2e/" || access.DependencyURL != filepath.Join(root, "dependency.json") {
		t.Fatalf("wrong access arguments: %+v", access)
	}
	if authored.BaseDir != "${Module}" || authored.ContentURL != "${Assets}" || authored.DependencyURL != "${Dependency}" {
		t.Fatal("loader replaced original configuration")
	}
}

func TestInstanceCLIFileSelectionOverridesConfig(t *testing.T) {
	root := t.TempDir()
	for name, text := range map[string]string{"e2e.yaml": "Project: e2e\nOnlyE2E: value\n", "prod.json": `{"Project":"prod"}`, "config.json": `{"ConstURL":"e2e.yaml","BaseDir":"."}`} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
	}
	cfg, err := (config.Loader{ConstURL: filepath.Join(root, "prod.json")}).Load(context.Background(), filepath.Join(root, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := cfg.Const.Lookup("Project"); got != "prod" {
		t.Fatal("CLI file did not override config")
	}
	if _, found := cfg.Const.Lookup("OnlyE2E"); found {
		t.Fatal("instance files were merged")
	}
	if cfg.ConstURL != "e2e.yaml" {
		t.Fatal("authored file selection overwritten")
	}
}
