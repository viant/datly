package developer_test

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/mcp/developer/skills"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/mcp/client"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"testing"
)

func TestDeveloperEmbeddedBinaryOutsideSource(t *testing.T) {
	expected := map[string][]byte{}
	for _, folder := range skills.Folders() {
		base, err := url.Parse(folder.URIPrefix)
		if err != nil {
			t.Fatal(err)
		}
		err = fs.WalkDir(folder.FS, folder.Root, func(name string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			uri := *base
			uri.Path = path.Join(uri.Path, strings.TrimPrefix(name, folder.Root+"/"))
			data, err := fs.ReadFile(folder.FS, name)
			if err != nil {
				return err
			}
			if _, exists := expected[uri.String()]; exists {
				t.Fatalf("duplicate embedded URI %s", uri.String())
			}
			expected[uri.String()] = data
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(expected) == 0 {
		t.Fatal("empty embedded skill bundle")
	}
	outside := t.TempDir()
	binary := filepath.Join(outside, "developer")
	build := exec.Command("go", "build", "-o", binary, "../../cmd/datly-developer")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build %v %s", err, output)
	}
	configuration := filepath.Join(outside, "developer.json")
	data, _ := json.Marshal(map[string]any{"Targets": map[string]any{"app": map[string]any{"BaseDir": outside, "Include": []string{"example.com/unused"}}}})
	if err := os.WriteFile(configuration, data, 0600); err != nil {
		t.Fatal(err)
	}
	// Child has no path to an authoring skill checkout; only the embedded bundle.
	t.Chdir(outside)
	for _, version := range []string{"2025-11-25", "2026-07-28"} {
		t.Run(version, func(t *testing.T) {
			native := (mcpclient.StdioConfig{Binary: binary, Args: []string{"-config", configuration}, ProtocolVersion: version}).New(t)
			init, err := native.Initialize(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			extension, ok := init.Capabilities.Extensions[schema.SkillsExtension]
			if !ok || len(extension) != 0 || init.Capabilities.Resources == nil {
				t.Fatalf("skills capability: %+v", init.Capabilities)
			}
			entries, err := native.ListSkills(context.Background(), nil)
			if err != nil || len(entries.Skills) != 3 {
				t.Fatalf("skills: %+v %v", entries, err)
			}
			if version == "2026-07-28" && (entries.TtlMs == nil || *entries.TtlMs != 0 || entries.CacheScope != "private") {
				t.Fatalf("July cache: %+v", entries)
			}
			if version == "2025-11-25" && (entries.TtlMs != nil || entries.CacheScope != "") {
				t.Fatalf("November cache leaked: %+v", entries)
			}
			total := 0
			for _, entry := range entries.Skills {
				total += len(entry.Resources.Files)
				got, err := native.GetSkill(context.Background(), entry.Uri)
				if err != nil || got.Skill.Uri != entry.Uri {
					t.Fatalf("get skill: %+v %v", got, err)
				}
				mcpclient.SkillFile(t, native, entry, entry.Uri)
			}
			if total != len(expected) {
				t.Fatalf("inventory count %d", total)
			}
			tools, err := native.ListTools(context.Background(), nil)
			if err != nil || len(tools.Tools) != 7 {
				t.Fatalf("business tool count %+v %v", tools, err)
			}
			listing, err := native.ListResources(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(listing.Resources) != len(expected) {
				t.Fatalf("bundle count=%d", len(listing.Resources))
			}
			for _, uri := range []string{"skill://datly-reader/SKILL.md", "skill://datly-writer/references/mutation-messages.md", "skill://datly-custom-component/agents/openai.yaml"} {
				read, err := native.ReadResource(context.Background(), &schema.ReadResourceRequestParams{Uri: uri})
				if err != nil || len(read.Contents) != 1 {
					t.Fatalf("%s: %v", uri, err)
				}
			}
			seen := map[string]bool{}
			for _, entry := range entries.Skills {
				for _, file := range entry.Resources.Files {
					want, exists := expected[file.Uri]
					if !exists || seen[file.Uri] {
						t.Fatalf("unexpected or duplicate inventory URI %s", file.Uri)
					}
					seen[file.Uri] = true
					got := mcpclient.SkillFile(t, native, entry, file.Uri)
					if !bytes.Equal(want, got) {
						t.Fatalf("embedded source mismatch for %s", file.Uri)
					}
				}
			}
			for _, uri := range []string{"", "skill://missing/SKILL.md", "skill://datly-reader/references/concepts.md", "skill://datly-reader/../SKILL.md"} {
				if _, err := native.GetSkill(context.Background(), uri); err == nil {
					t.Fatalf("accepted non-skill URI %q", uri)
				}
			}
		})
	}
	privateConfiguration := filepath.Join(outside, "private.json")
	privateData, _ := json.Marshal(map[string]any{"Targets": map[string]any{"app": map[string]any{"BaseDir": outside}}, "Authorization": map[string]any{"global": map[string]any{"requiredScopes": []string{"skills"}, "protectedResourceMetadata": map[string]any{"resource": "https://private.example"}}}})
	if err := os.WriteFile(privateConfiguration, privateData, 0600); err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{"2025-11-25", "2026-07-28"} {
		t.Run("private_"+version, func(t *testing.T) {
			native := (mcpclient.StdioConfig{Binary: binary, Args: []string{"-config", privateConfiguration}, ProtocolVersion: version}).New(t)
			for _, token := range []string{"", "forged"} {
				option := client.WithAuthToken(token)
				if _, err := native.ListSkills(context.Background(), nil, option); err == nil {
					t.Fatal("private skill metadata leaked")
				}
				if _, err := native.GetSkill(context.Background(), "skill://datly-reader/SKILL.md", option); err == nil {
					t.Fatal("private skill entry leaked")
				}
				if _, err := native.ListResources(context.Background(), nil, option); err == nil {
					t.Fatal("private resource metadata leaked")
				}
				if _, err := native.ReadResource(context.Background(), &schema.ReadResourceRequestParams{Uri: "skill://datly-reader/SKILL.md"}, option); err == nil {
					t.Fatal("private skill bytes leaked")
				}
			}
		})
	}
}
