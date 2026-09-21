package spec

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestCacheSettingsEffectiveWarmups_Ordering(t *testing.T) {
	singular := &CacheWarmupSettings{IndexColumn: "advertiser_id"}
	campaign := &CacheWarmupSettings{IndexColumn: "campaign_id"}
	order := &CacheWarmupSettings{IndexColumn: "order_id"}

	var nilSettings *CacheSettings
	if warmups, err := nilSettings.EffectiveWarmups(); err != nil || warmups != nil {
		t.Fatalf("nil settings: %v %v", warmups, err)
	}
	if nilSettings.HasWarmup() {
		t.Fatal("nil settings must have no warmup")
	}

	names := func(settings *CacheSettings) []string {
		warmups, err := settings.EffectiveWarmups()
		if err != nil {
			t.Fatalf("effective warmups: %v", err)
		}
		var result []string
		for _, item := range warmups {
			result = append(result, item.EffectiveName())
		}
		return result
	}
	if actual := names(&CacheSettings{Warmup: singular}); !reflect.DeepEqual(actual, []string{"advertiser_id"}) {
		t.Fatalf("singular only: %v", actual)
	}
	if actual := names(&CacheSettings{Warmups: []*CacheWarmupSettings{campaign, nil, order}}); !reflect.DeepEqual(actual, []string{"campaign_id", "order_id"}) {
		t.Fatalf("plural only: %v", actual)
	}
	if actual := names(&CacheSettings{Warmup: singular, Warmups: []*CacheWarmupSettings{campaign, order}}); !reflect.DeepEqual(actual, []string{"advertiser_id", "campaign_id", "order_id"}) {
		t.Fatalf("mixed: %v", actual)
	}
	// An empty plural collection does not disable a valid singular warmup.
	withEmptyPlural := &CacheSettings{Warmup: singular, Warmups: []*CacheWarmupSettings{}}
	if actual := names(withEmptyPlural); !reflect.DeepEqual(actual, []string{"advertiser_id"}) {
		t.Fatalf("empty plural shadows singular: %v", actual)
	}
	if !withEmptyPlural.HasWarmup() {
		t.Fatal("expected warmup presence")
	}
}

func TestCacheWarmupSettingsEffectiveName(t *testing.T) {
	if name := (&CacheWarmupSettings{Name: "campaign", IndexParameter: "CampaignIds", IndexColumn: "campaign_id"}).EffectiveName(); name != "campaign" {
		t.Fatalf("name=%v", name)
	}
	if name := (&CacheWarmupSettings{IndexParameter: "CampaignIds", IndexColumn: "campaign_id"}).EffectiveName(); name != "CampaignIds" {
		t.Fatalf("name=%v", name)
	}
	if name := (&CacheWarmupSettings{IndexColumn: "campaign_id"}).EffectiveName(); name != "campaign_id" {
		t.Fatalf("name=%v", name)
	}
	var nilWarmup *CacheWarmupSettings
	if name := nilWarmup.EffectiveName(); name != "" {
		t.Fatalf("name=%v", name)
	}
}

func TestEffectiveWarmups_RejectsDuplicates(t *testing.T) {
	duplicateName := &CacheSettings{
		Warmup:  &CacheWarmupSettings{IndexColumn: "advertiser_id", IndexParameter: "AdvertiserIds"},
		Warmups: []*CacheWarmupSettings{{Name: "AdvertiserIds", IndexColumn: "campaign_id"}},
	}
	if _, err := duplicateName.EffectiveWarmups(); err == nil || !strings.Contains(err.Error(), "duplicate warmup name") {
		t.Fatalf("expected duplicate name error, got %v", err)
	}
	duplicateIndex := &CacheSettings{
		Warmup:  &CacheWarmupSettings{Name: "broad", IndexColumn: "advertiser_id", IndexParameter: "AdvertiserIds"},
		Warmups: []*CacheWarmupSettings{{Name: "duplicate", IndexColumn: "advertiser_id", IndexParameter: "AdvertiserIds"}},
	}
	if _, err := duplicateIndex.EffectiveWarmups(); err == nil || !strings.Contains(err.Error(), "duplicate warmup index identity") {
		t.Fatalf("expected duplicate index identity error, got %v", err)
	}
	unknownRef := &CacheSettings{Warmup: &CacheWarmupSettings{IndexColumn: "advertiser_id", CaseRefs: []string{"missing"}}}
	if _, err := unknownRef.EffectiveWarmups(); err == nil || !strings.Contains(err.Error(), "not found warmup case set") {
		t.Fatalf("expected unknown case set error, got %v", err)
	}
}

func TestEffectiveWarmups_ExpandsCaseRefsPerWarmup(t *testing.T) {
	recent := []*CacheWarmupCase{
		{Set: []*CacheWarmupParam{{Name: "Period", Values: []string{"today", "yesterday"}, ExcludeDefault: true}}},
	}
	settings := &CacheSettings{
		SharedCases: map[string][]*CacheWarmupCase{"recent": recent},
		Warmup: &CacheWarmupSettings{
			IndexColumn: "advertiser_id",
			CaseRefs:    []string{"recent"},
			Cases: []*CacheWarmupCase{
				{Set: []*CacheWarmupParam{{Name: "Period", Values: []string{"lastweek"}, ExcludeDefault: true}}},
			},
		},
		Warmups: []*CacheWarmupSettings{
			{IndexColumn: "campaign_id", CaseRefs: []string{"recent"}},
		},
	}
	warmups, err := settings.EffectiveWarmups()
	if err != nil || len(warmups) != 2 {
		t.Fatalf("warmups=%d err=%v", len(warmups), err)
	}
	// Referenced cases come first, inline cases follow.
	if len(warmups[0].Cases) != 2 || warmups[0].Cases[0].Set[0].Values[0] != "today" || warmups[0].Cases[1].Set[0].Values[0] != "lastweek" {
		t.Fatalf("singular cases=%+v", warmups[0].Cases)
	}
	if len(warmups[1].Cases) != 1 {
		t.Fatalf("plural cases=%+v", warmups[1].Cases)
	}
	// Expansion never shares mutable case slices: mutating one warmup's expanded
	// cases must not leak into the other warmup, the shared set, or the source.
	warmups[0].Cases[0].Set[0].Values[0] = "mutated"
	if warmups[1].Cases[0].Set[0].Values[0] != "today" || recent[0].Set[0].Values[0] != "today" {
		t.Fatal("expanded cases share mutable state")
	}
	if len(settings.Warmup.Cases) != 1 {
		t.Fatal("normalization mutated the declared metadata")
	}
	// Duplicate cases within one warmup collapse; the same case on two warmups
	// is retained because index strategy is part of cache identity.
	settings.Warmup.Cases = append(settings.Warmup.Cases, recent[0].Clone())
	warmups, err = settings.EffectiveWarmups()
	if err != nil || len(warmups[0].Cases) != 2 {
		t.Fatalf("expected deduped cases, got %+v err=%v", warmups[0].Cases, err)
	}
}

func TestCacheSettingsClone_DeepCopiesWarmups(t *testing.T) {
	source := &CacheSettings{
		Name:   "cube",
		Warmup: &CacheWarmupSettings{Name: "broad", Priority: 1, IndexColumn: "advertiser_id", CaseRefs: []string{"recent"}},
		Warmups: []*CacheWarmupSettings{
			{IndexColumn: "campaign_id", Cases: []*CacheWarmupCase{{Set: []*CacheWarmupParam{{Name: "Period", Values: []string{"today"}}}}}},
		},
		SharedCases: map[string][]*CacheWarmupCase{
			"recent": {{Set: []*CacheWarmupParam{{Name: "Period", Values: []string{"today"}}}}},
		},
	}
	cloned := source.Clone()
	cloned.Warmup.CaseRefs[0] = "mutated"
	cloned.Warmups[0].Cases[0].Set[0].Values[0] = "mutated"
	cloned.SharedCases["recent"][0].Set[0].Values[0] = "mutated"
	if source.Warmup.CaseRefs[0] != "recent" || source.Warmups[0].Cases[0].Set[0].Values[0] != "today" || source.SharedCases["recent"][0].Set[0].Values[0] != "today" {
		t.Fatal("clone shares mutable state with source")
	}
}

// TestCacheWarmupFixtureCompatibility loads the shared original-Datly style
// JSON and YAML fixtures and verifies both normalize to the same effective
// warmups with an identical canonical fingerprint.
func TestCacheWarmupFixtureCompatibility(t *testing.T) {
	jsonData, err := os.ReadFile(filepath.Join("testdata", "cache_warmups_mixed.json"))
	if err != nil {
		t.Fatal(err)
	}
	fromJSON := &CacheSettings{}
	if err := json.Unmarshal(jsonData, fromJSON); err != nil {
		t.Fatal(err)
	}
	yamlData, err := os.ReadFile(filepath.Join("testdata", "cache_warmups_mixed.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	fromYAML := &CacheSettings{}
	if err := yaml.Unmarshal(yamlData, fromYAML); err != nil {
		t.Fatal(err)
	}
	fingerprint := func(settings *CacheSettings) string {
		warmups, err := settings.EffectiveWarmups()
		if err != nil {
			t.Fatalf("effective warmups: %v", err)
		}
		data, err := json.Marshal(warmups)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	jsonPrint, yamlPrint := fingerprint(fromJSON), fingerprint(fromYAML)
	if jsonPrint != yamlPrint {
		t.Fatalf("fingerprints differ:\njson=%s\nyaml=%s", jsonPrint, yamlPrint)
	}
	warmups, err := fromJSON.EffectiveWarmups()
	if err != nil || len(warmups) != 2 {
		t.Fatalf("warmups=%d err=%v", len(warmups), err)
	}
	if warmups[0].EffectiveName() != "AdvertiserID" || warmups[1].EffectiveName() != "campaign" {
		t.Fatalf("names=%v,%v", warmups[0].EffectiveName(), warmups[1].EffectiveName())
	}
	// Singular expansion: recent, broad, then the inline case.
	if len(warmups[0].Cases) != 3 || warmups[0].Cases[2].Set[0].Values[0] != "lastweek" {
		t.Fatalf("singular cases=%+v", warmups[0].Cases)
	}
	campaign := warmups[1]
	if campaign.Priority != 2 || campaign.Limit == nil || *campaign.Limit != 100 || campaign.MaxCases == nil || *campaign.MaxCases != 30 {
		t.Fatalf("campaign=%+v", campaign)
	}
	if !reflect.DeepEqual(campaign.FieldNames, []string{"id", "spend"}) || len(campaign.Cases) != 1 {
		t.Fatalf("campaign projection/cases=%+v", campaign)
	}
}
