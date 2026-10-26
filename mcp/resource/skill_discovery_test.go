package resource

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/viant/mcp-protocol/schema"
)

func discoveryFixture() []schema.Skill {
	result := []schema.Skill{}
	for _, item := range []struct{ name, group, description string }{{"read", "records", "Read records"}, {"write", "records", "Write records"}, {"audit", "audit", "Inspect changes"}} {
		result = append(result, schema.Skill{Uri: "skill://catalog/" + item.name + "/SKILL.md", Frontmatter: map[string]interface{}{"name": item.name, "group": item.group, "description": item.description}, Resources: schema.SkillResources{Files: []schema.SkillResource{{Uri: "skill://catalog/" + item.name + "/references/contract.md", Digest: "sha256:test", Size: 100}}}})
	}
	return result
}

func TestDiscoverCompactGroupsAndFilters(t *testing.T) {
	entries := discoveryFixture()
	result, err := Discover(entries, DiscoveryOptions{Group: "records", Query: " RECORDS ", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 2 || len(result.Skills) != 1 || result.NextCursor == nil || len(result.Groups) != 1 || result.Groups[0].ResourceCount != 2 {
		t.Fatalf("unexpected result: %+v", result)
	}
	raw, _ := json.Marshal(result)
	if strings.Contains(string(raw), "contract.md") || strings.Contains(string(raw), "sha256:") {
		t.Fatal("compact discovery leaked full references")
	}
	if result.CacheScope != "private" || result.TTLMillis != 0 {
		t.Fatal("discovery cache must be caller private and nonreusable")
	}
	next, err := Discover(entries, DiscoveryOptions{Group: "records", Query: "records", Limit: 1, Cursor: result.NextCursor})
	if err != nil || len(next.Skills) != 1 || next.NextCursor != nil || next.Skills[0].URI == result.Skills[0].URI {
		t.Fatalf("pagination: %+v %v", next, err)
	}
	if len(entries[0].Resources.Files) != 1 {
		t.Fatal("discovery mutated full inventory")
	}
}

func TestDiscoverRejectsCursorAcrossFiltersScopeAndRevision(t *testing.T) {
	entries := discoveryFixture()
	first, err := Discover(entries, DiscoveryOptions{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, options := range []DiscoveryOptions{{Limit: 2, Cursor: first.NextCursor}, {Limit: 1, Group: "records", Cursor: first.NextCursor}, {Limit: 1, Query: "read", Cursor: first.NextCursor}} {
		if _, err := Discover(entries, options); err == nil {
			t.Fatal("accepted cursor with changed filter")
		}
	}
	if _, err := Discover(entries[:2], DiscoveryOptions{Limit: 1, Cursor: first.NextCursor}); err == nil {
		t.Fatal("accepted cursor from another visible catalog")
	}
	entries[0].Resources.Files[0].Digest = "sha256:changed"
	if _, err := Discover(entries, DiscoveryOptions{Limit: 1, Cursor: first.NextCursor}); err == nil {
		t.Fatal("accepted stale inventory cursor")
	}
}

func TestDiscoverValidationAndEmptyResults(t *testing.T) {
	for _, options := range []DiscoveryOptions{{Limit: -1}, {Limit: 101}, {Query: strings.Repeat("x", 513)}, {Group: strings.Repeat("x", 257)}} {
		if _, err := Discover(nil, options); err == nil {
			t.Fatal("accepted invalid options")
		}
	}
	result, err := Discover(discoveryFixture(), DiscoveryOptions{Group: "missing"})
	if err != nil || result.Total != 0 || result.Skills == nil || result.Groups == nil || result.NextCursor != nil {
		t.Fatalf("empty result: %+v %v", result, err)
	}
}

func TestDiscoverTargetedResourcesAndCursorBinding(t *testing.T) {
	entries := discoveryFixture()
	entry := &entries[0]
	entry.Resources.Files = append(entry.Resources.Files, schema.SkillResource{Uri: "skill://catalog/read/references/chart.schema.json", Digest: "sha256:chart", Size: 42}, schema.SkillResource{Uri: "skill://catalog/read/references/chart-index.md", Digest: "sha256:index", Size: 24})
	options := DiscoveryOptions{Kind: "resources", SkillURI: entry.Uri, Query: "CHART", Prefix: "skill://catalog/read/references/", Limit: 1}
	first, err := Discover(entries, options)
	if err != nil {
		t.Fatal(err)
	}
	if first.Total != 2 || len(first.Resources) != 1 || len(first.Skills) != 0 || first.NextCursor == nil {
		t.Fatalf("resource result: %+v", first)
	}
	if first.Resources[0].Digest == "" || first.Resources[0].Size == 0 {
		t.Fatal("resource descriptor lost exact metadata")
	}
	options.Cursor = first.NextCursor
	second, err := Discover(entries, options)
	if err != nil || len(second.Resources) != 1 || second.NextCursor != nil || second.Resources[0].Uri == first.Resources[0].Uri {
		t.Fatalf("resource next page: %+v %v", second, err)
	}
	for _, changed := range []DiscoveryOptions{{Kind: "skills", Limit: 1, Cursor: first.NextCursor}, {Kind: "resources", SkillURI: entry.Uri, Limit: 1, Cursor: first.NextCursor}, {Kind: "resources", SkillURI: entries[1].Uri, Query: "CHART", Prefix: options.Prefix, Limit: 1, Cursor: first.NextCursor}, {Kind: "resources", SkillURI: entry.Uri, Query: "CHART", Prefix: "changed", Limit: 1, Cursor: first.NextCursor}} {
		if _, err := Discover(entries, changed); err == nil {
			t.Fatal("resource cursor accepted changed mode or filters")
		}
	}
	none, err := Discover(entries, DiscoveryOptions{Kind: "resources", SkillURI: "skill://catalog/unknown/SKILL.md"})
	if err != nil || none.Total != 0 {
		t.Fatalf("unknown skill: %+v %v", none, err)
	}
	for _, bad := range []DiscoveryOptions{{Kind: "unknown"}, {Kind: "skills", SkillURI: entry.Uri}, {Prefix: "skill://catalog/"}} {
		if _, err := Discover(entries, bad); err == nil {
			t.Fatal("invalid mode/filter accepted")
		}
	}
}

func TestDiscoveryOptionsRejectMalformedFilters(t *testing.T) {
	for _, raw := range []string{`{"kind":null}`, `{"query":null}`, `{"limit":1.5}`, `{"limit":"1"}`, `{"skill_uri":"misspelled"}`, `{"unrecognized":true}`} {
		var options DiscoveryOptions
		if err := json.Unmarshal([]byte(raw), &options); err == nil {
			t.Fatalf("accepted malformed filters %s", raw)
		}
	}
}
