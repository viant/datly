package runtime

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
)

type pluralWarmupInput struct {
	AdvertiserID int
	CampaignID   int
}

type pluralWarmupRow struct {
	AdvertiserID int `sqlx:"advertiser_id"`
	CampaignID   int `sqlx:"campaign_id"`
	Value        int `sqlx:"value"`
}

type pluralWarmupOutput struct{ Rows []pluralWarmupRow }

const pluralWarmupSQL = "SELECT advertiser_id, campaign_id, value FROM records WHERE (:AdvertiserID=0 OR advertiser_id=:AdvertiserID) AND (:CampaignID=0 OR campaign_id=:CampaignID) ORDER BY value"

func newPluralWarmupRuntime(t *testing.T, ctx context.Context, cache *spec.CacheSettings, parameters []*spec.Parameter) (*Runtime, dexec.ComponentTarget, *sqlite.Harness) {
	t.Helper()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx,
		"CREATE TABLE records(advertiser_id INTEGER, campaign_id INTEGER, value INTEGER)",
		"INSERT INTO records VALUES(101,7,1),(101,8,2),(202,9,3)",
	); err != nil {
		t.Fatal(err)
	}
	cache.Enabled = true
	cache.Name = "records"
	cache.Location = t.TempDir()
	cache.TTL = "1m"
	if parameters == nil {
		parameters = []*spec.Parameter{
			{Name: "AdvertiserID", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "advertiserId"}},
			{Name: "CampaignID", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "campaignId"}},
		}
	}
	component := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Name: "PluralRecords"}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}},
		Settings:   &spec.Settings{Cache: cache},
		Parameters: append(parameters, &spec.Parameter{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}),
		RootView:   &spec.View{Name: "records", Source: &spec.ViewSource{SQL: pluralWarmupSQL}},
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(pluralWarmupInput{}), OutputType: reflect.TypeOf(pluralWarmupOutput{}), DirectViewField: "Rows"})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(pluralWarmupOutput{}), Reader: reader}})
	if err != nil {
		t.Fatal(err)
	}
	return runtime, dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "GET", Path: "/records"}}, db
}

func pluralWarmupRead(t *testing.T, ctx context.Context, runtime *Runtime, target dexec.ComponentTarget, bound *pluralWarmupInput, want []pluralWarmupRow) {
	t.Helper()
	actual, err := runtime.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Input: bound})
	if err != nil {
		t.Fatalf("warmup replay missed and attempted root SQL: %v", err)
	}
	rows := actual.(*pluralWarmupOutput).Rows
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows=%v, want %v", rows, want)
	}
}

// TestRuntimePluralIndexedWarmupSQLite proves plural-only and mixed
// singular-plus-plural configurations execute every effective warmup and each
// index replays with the protected source unavailable.
func TestRuntimePluralIndexedWarmupSQLite(t *testing.T) {
	advertiser := &spec.CacheWarmupSettings{IndexColumn: "advertiser_id", IndexParameter: "AdvertiserID"}
	campaign := &spec.CacheWarmupSettings{IndexColumn: "campaign_id", IndexParameter: "CampaignID"}
	for _, tc := range []struct {
		name  string
		cache *spec.CacheSettings
	}{
		{"plural only", &spec.CacheSettings{Warmups: []*spec.CacheWarmupSettings{advertiser.Clone(), campaign.Clone()}}},
		{"mixed singular and plural", &spec.CacheSettings{Warmup: advertiser.Clone(), Warmups: []*spec.CacheWarmupSettings{campaign.Clone()}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			runtime, target, db := newPluralWarmupRuntime(t, ctx, tc.cache, nil)
			// Two advertiser groups plus three campaign groups: every effective
			// warmup executes once and owns its generated work.
			if count, err := runtime.Warmup(ctx, target); err != nil || count != 5 {
				t.Fatalf("Warmup=%d,%v", count, err)
			}
			if err := db.ExecStatements(ctx, "DROP TABLE records"); err != nil {
				t.Fatal(err)
			}
			pluralWarmupRead(t, ctx, runtime, target, &pluralWarmupInput{AdvertiserID: 101}, []pluralWarmupRow{{101, 7, 1}, {101, 8, 2}})
			pluralWarmupRead(t, ctx, runtime, target, &pluralWarmupInput{CampaignID: 9}, []pluralWarmupRow{{202, 9, 3}})
		})
	}
}

// TestRuntimePluralWarmupSelectionSQLite proves request lookup selects the most
// restrictive supplied index: the later broad-to-specific declaration wins on
// equal priority and an explicit higher priority overrides declaration order.
func TestRuntimePluralWarmupSelectionSQLite(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cache *spec.CacheSettings
		want  []pluralWarmupRow
	}{
		{
			// Both indexes are supplied; only the later campaign warmup was
			// warmed with the retained AdvertiserID=101 filter, so a hit proves
			// the later declaration was selected.
			name: "later declaration wins equal priority",
			cache: &spec.CacheSettings{Warmups: []*spec.CacheWarmupSettings{
				{IndexColumn: "advertiser_id", IndexParameter: "AdvertiserID"},
				{IndexColumn: "campaign_id", IndexParameter: "CampaignID",
					Cases: []*spec.CacheWarmupCase{{Set: []*spec.CacheWarmupParam{{Name: "AdvertiserID", Values: []string{"101"}, ExcludeDefault: true}}}}},
			}},
			want: []pluralWarmupRow{{101, 7, 1}},
		},
		{
			// Only the earlier advertiser warmup carries Priority and was warmed
			// with the retained CampaignID=7 filter, so a hit proves explicit
			// priority overrode the later declaration.
			name: "explicit priority wins",
			cache: &spec.CacheSettings{Warmups: []*spec.CacheWarmupSettings{
				{Priority: 1, IndexColumn: "advertiser_id", IndexParameter: "AdvertiserID",
					Cases: []*spec.CacheWarmupCase{{Set: []*spec.CacheWarmupParam{{Name: "CampaignID", Values: []string{"7"}, ExcludeDefault: true}}}}},
				{IndexColumn: "campaign_id", IndexParameter: "CampaignID"},
			}},
			want: []pluralWarmupRow{{101, 7, 1}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			runtime, target, db := newPluralWarmupRuntime(t, ctx, tc.cache, nil)
			if count, err := runtime.Warmup(ctx, target); err != nil || count == 0 {
				t.Fatalf("Warmup=%d,%v", count, err)
			}
			if err := db.ExecStatements(ctx, "DROP TABLE records"); err != nil {
				t.Fatal(err)
			}
			pluralWarmupRead(t, ctx, runtime, target, &pluralWarmupInput{AdvertiserID: 101, CampaignID: 7}, tc.want)
		})
	}
}

// TestRuntimePluralWarmupRequiredIndexOmissionSQLite proves required index
// input omission applies to the selected warmup only: each warmup must satisfy
// the other warmup's required parameter through its own cases.
func TestRuntimePluralWarmupRequiredIndexOmissionSQLite(t *testing.T) {
	required := true
	parameters := []*spec.Parameter{
		{Name: "AdvertiserID", Required: &required, TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "advertiserId"}},
		{Name: "CampaignID", Required: &required, TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "campaignId"}},
	}
	ctx := context.Background()
	cache := &spec.CacheSettings{Warmups: []*spec.CacheWarmupSettings{
		{IndexColumn: "advertiser_id", IndexParameter: "AdvertiserID",
			Cases: []*spec.CacheWarmupCase{{Set: []*spec.CacheWarmupParam{{Name: "CampaignID", Values: []string{"0"}, ExcludeDefault: true}}}}},
		{IndexColumn: "campaign_id", IndexParameter: "CampaignID",
			Cases: []*spec.CacheWarmupCase{{Set: []*spec.CacheWarmupParam{{Name: "AdvertiserID", Values: []string{"0"}, ExcludeDefault: true}}}}},
	}}
	runtime, target, db := newPluralWarmupRuntime(t, ctx, cache, parameters)
	if count, err := runtime.Warmup(ctx, target); err != nil || count != 5 {
		t.Fatalf("Warmup=%d,%v", count, err)
	}
	if err := db.ExecStatements(ctx, "DROP TABLE records"); err != nil {
		t.Fatal(err)
	}
	pluralWarmupRead(t, ctx, runtime, target, &pluralWarmupInput{CampaignID: 7}, []pluralWarmupRow{{101, 7, 1}})

	// Without cases the other warmup's required index input stays required, so
	// the omission provably applies only to the executing warmup's own index.
	bare := &spec.CacheSettings{Warmups: []*spec.CacheWarmupSettings{
		{IndexColumn: "advertiser_id", IndexParameter: "AdvertiserID"},
		{IndexColumn: "campaign_id", IndexParameter: "CampaignID"},
	}}
	failing, failingTarget, _ := newPluralWarmupRuntime(t, ctx, bare, []*spec.Parameter{
		{Name: "AdvertiserID", Required: &required, TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "advertiserId"}},
		{Name: "CampaignID", Required: &required, TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "campaignId"}},
	})
	if _, err := failing.Warmup(ctx, failingTarget); err == nil {
		t.Fatal("expected warmup to require the non-omitted index input")
	}
}

// TestRuntimePluralWarmupSharedCasesPlanned proves shared case sets expand
// independently for every referencing warmup with no undeclared cartesian
// product between cases and indexes.
func TestRuntimePluralWarmupSharedCasesPlanned(t *testing.T) {
	ctx := context.Background()
	cache := &spec.CacheSettings{
		SharedCases: map[string][]*spec.CacheWarmupCase{
			"recent": {{Set: []*spec.CacheWarmupParam{{Name: "CampaignID", Values: []string{"7", "8"}, ExcludeDefault: true}}}},
		},
		Warmups: []*spec.CacheWarmupSettings{
			{IndexColumn: "advertiser_id", IndexParameter: "AdvertiserID", CaseRefs: []string{"recent"}},
			{IndexColumn: "campaign_id", IndexParameter: "CampaignID", CaseRefs: []string{"recent"}},
		},
	}
	runtime, target, _ := newPluralWarmupRuntime(t, ctx, cache, nil)
	operation, err := runtime.NewWarmup(target)
	if err != nil {
		t.Fatal(err)
	}
	// Two shared values per warmup, two warmups: four planned cases exactly.
	if planned, err := operation.PlannedCases(); err != nil || planned != 4 {
		t.Fatalf("PlannedCases=%d,%v", planned, err)
	}
}

// TestRuntimePluralWarmupRejectsDuplicateIdentity proves duplicate effective
// warmup identities fail reader initialization instead of overwriting.
func TestRuntimePluralWarmupRejectsDuplicateIdentity(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(advertiser_id INTEGER, campaign_id INTEGER, value INTEGER)"); err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Name: "DuplicateWarmups"}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}},
		Settings: &spec.Settings{Cache: &spec.CacheSettings{
			Enabled: true, Name: "records", Location: t.TempDir(), TTL: "1m",
			Warmup:  &spec.CacheWarmupSettings{IndexColumn: "advertiser_id", IndexParameter: "AdvertiserID"},
			Warmups: []*spec.CacheWarmupSettings{{IndexColumn: "advertiser_id", IndexParameter: "AdvertiserID"}},
		}},
		Parameters: []*spec.Parameter{
			{Name: "AdvertiserID", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "advertiserId"}},
			{Name: "CampaignID", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "campaignId"}},
			{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
		RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: pluralWarmupSQL}},
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(pluralWarmupInput{}), OutputType: reflect.TypeOf(pluralWarmupOutput{}), DirectViewField: "Rows"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
	if err == nil || !strings.Contains(err.Error(), "duplicate warmup") {
		t.Fatalf("expected duplicate warmup rejection, got %v", err)
	}
}
