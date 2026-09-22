package runtime

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	xhandler "github.com/viant/xdatly/handler"
	xstate "github.com/viant/xdatly/state"
)

func TestRuntimeIndexedWarmupPreservesRootWindowSQLite(t *testing.T) {
	type input struct {
		ID   int
		Keys []int
	}
	type row struct {
		ID    int `sqlx:"id"`
		Value int `sqlx:"value"`
	}
	type output struct{ Rows []row }
	for _, tc := range []struct {
		name     string
		selector xstate.Selector
		want     []row
		multiple bool
	}{
		{"default", xstate.Selector{}, []row{{11, 1}, {11, 2}}, false},
		{"limit", xstate.Selector{Limit: 1}, []row{{11, 1}}, false},
		{"offset", xstate.Selector{Limit: 1, Offset: 2}, []row{{11, 3}}, false},
		{"page", xstate.Selector{Limit: 1, Page: 2}, []row{{11, 2}}, false},
		{"clamped", xstate.Selector{Limit: 10}, []row{{11, 1}, {11, 2}}, false},
		{"multiple indexes use SQL global window", xstate.Selector{Limit: 1, Offset: 1}, []row{{11, 12}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER, value INTEGER)", "INSERT INTO records VALUES(11,1),(11,2),(11,3),(22,4)"); err != nil {
				t.Fatal(err)
			}
			component := &spec.Component{
				Key: spec.Key{Kind: spec.KindComponent, Name: "WindowRecords"}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}},
				Settings:   &spec.Settings{Cache: &spec.CacheSettings{Enabled: true, Name: "records", Location: t.TempDir(), TTL: "1m", Warmup: &spec.CacheWarmupSettings{IndexColumn: "id", IndexParameter: "ID"}}},
				Parameters: []*spec.Parameter{{Name: "ID", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "id"}}, {Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}},
				RootView:   &spec.View{Name: "records", Selector: &spec.Selector{DefaultLimit: 2, AllowLimit: true, AllowOffset: true, AllowPage: true}, Source: &spec.ViewSource{SQL: "SELECT id, value FROM records WHERE (:ID=0 OR id=:ID) ORDER BY value"}},
			}
			bound := &input{ID: 11}
			if tc.multiple {
				// Select every indexed group. Replaying a per-key offset/limit
				// would differ from this root's single SQL window.
				component.RootView.Source.SQL = "SELECT id, value FROM records ORDER BY value"
				component.Settings.Cache.Warmup.IndexParameter = "Keys"
				component.Parameters = append(component.Parameters, &spec.Parameter{Name: "Keys", TypeExpr: "[]int", Source: spec.BindSource{Kind: "query", Name: "keys"}})
				bound = &input{Keys: []int{11, 22}}
			}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Rows"})
			if err != nil {
				t.Fatal(err)
			}
			reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
			if err != nil {
				t.Fatal(err)
			}
			runtime, err := NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(output{}), Reader: reader}})
			if err != nil {
				t.Fatal(err)
			}
			target := dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "GET", Path: "/records"}}
			if count, err := runtime.Warmup(ctx, target); err != nil || count != 2 {
				t.Fatalf("Warmup=%d,%v", count, err)
			}
			statement := "DROP TABLE records"
			if tc.multiple {
				// Changed values prove this uses SQL, not the warmed records.
				statement = "UPDATE records SET value=value+10"
			}
			if err := db.ExecStatements(ctx, statement); err != nil {
				t.Fatal(err)
			}
			actual, err := runtime.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Input: bound, Providers: []locator.Provider{handlerprovider.Static(xhandler.SelectorsKey, xstate.Selectors{&xstate.NamedSelector{Name: "records", Selector: tc.selector}})}})
			if err != nil {
				t.Fatal(err)
			}
			if rows := actual.(*output).Rows; !reflect.DeepEqual(rows, tc.want) {
				t.Fatalf("rows=%v,want %v", rows, tc.want)
			}
		})
	}
}

func TestRuntimeIndexedWarmupSQLite(t *testing.T) {
	type input struct{ ID int }
	type row struct {
		ID int `sqlx:"id"`
	}
	type output struct{ Rows []row }
	for _, tc := range []struct {
		name string
		id   int
		want []row
	}{{"first", 11, []row{{11}}}, {"second", 22, []row{{22}}}, {"absent", 33, []row{}}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(11),(22)"); err != nil {
				t.Fatal(err)
			}
			component := &spec.Component{
				Key: spec.Key{Kind: spec.KindComponent, Name: "Records"}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}},
				Settings:   &spec.Settings{Cache: &spec.CacheSettings{Enabled: true, Name: "records", Location: t.TempDir(), TTL: "1m", Warmup: &spec.CacheWarmupSettings{IndexColumn: "id", IndexParameter: "ID"}}},
				Parameters: []*spec.Parameter{{Name: "ID", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "id"}}, {Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}},
				RootView:   &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id FROM records WHERE (:ID=0 OR id=:ID) ORDER BY id"}},
			}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Rows"})
			if err != nil {
				t.Fatal(err)
			}
			reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
			if err != nil {
				t.Fatal(err)
			}
			runtime, err := NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(output{}), Reader: reader}})
			if err != nil {
				t.Fatal(err)
			}
			target := dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "GET", Path: "/records"}}
			if count, err := runtime.Warmup(ctx, target); err != nil || count != 2 {
				t.Fatalf("Warmup=%d,%v", count, err)
			}
			if err := db.ExecStatements(ctx, "DROP TABLE records"); err != nil {
				t.Fatal(err)
			}
			bound := &input{ID: tc.id}
			actual, err := runtime.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Input: bound})
			if err != nil {
				t.Fatal(err)
			}
			rows := actual.(*output).Rows
			if len(rows) != 0 || len(tc.want) != 0 {
				if !reflect.DeepEqual(rows, tc.want) {
					t.Fatalf("rows=%v,want %v", rows, tc.want)
				}
			}
			if bound.ID != tc.id {
				t.Fatal("cache identity preparation mutated canonical input")
			}
		})
	}
}

func TestRuntimeIndexedWarmupOmitsRequiredIndexParameterSQLite(t *testing.T) {
	type input struct{ CampaignID int }
	type row struct {
		CampaignID int `sqlx:"campaign_id"`
		Value      int `sqlx:"value"`
	}
	type output struct{ Rows []row }

	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx,
		"CREATE TABLE campaign_report(campaign_id INTEGER, value INTEGER)",
		"INSERT INTO campaign_report VALUES(101,7),(202,9)",
	); err != nil {
		t.Fatal(err)
	}
	required := true
	component := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Name: "CampaignReport"}, Routes: []*spec.Route{{Method: "GET", Path: "/campaign"}},
		Settings: &spec.Settings{Cache: &spec.CacheSettings{
			Enabled: true, Name: "campaign_report", Location: t.TempDir(), TTL: "1m",
			Warmup: &spec.CacheWarmupSettings{IndexColumn: "campaign_id", IndexParameter: "campaign_id"},
		}},
		Parameters: []*spec.Parameter{
			{Name: "CampaignID", Required: &required, TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "campaign_id"}},
			{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
		RootView: &spec.View{Name: "campaign_report", Source: &spec.ViewSource{SQL: "SELECT campaign_id, value FROM campaign_report WHERE (:CampaignID=0 OR campaign_id=:CampaignID) ORDER BY campaign_id"}},
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Rows"})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(output{}), Reader: reader}})
	if err != nil {
		t.Fatal(err)
	}
	target := dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "GET", Path: "/campaign"}}

	if _, err = runtime.InvokeComponent(ctx, dexec.ComponentRequest{Target: target}); err == nil {
		t.Fatal("ordinary read accepted missing required campaign_id")
	}
	if count, err := runtime.Warmup(ctx, target); err != nil || count != 2 {
		t.Fatalf("Warmup=%d,%v", count, err)
	}
	if err := db.ExecStatements(ctx, "DROP TABLE campaign_report"); err != nil {
		t.Fatal(err)
	}
	actual, err := runtime.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Input: &input{CampaignID: 101}})
	if err != nil {
		t.Fatalf("warmup replay missed and attempted root SQL: %v", err)
	}
	rows := actual.(*output).Rows
	if len(rows) != 1 || rows[0] != (row{CampaignID: 101, Value: 7}) {
		t.Fatalf("rows=%+v", rows)
	}
}

func TestRuntimeIndexedGroupedWarmupReusesNamedPlaceholderDimensionsSQLite(t *testing.T) {
	type input struct{ AdvertiserID int }
	type row struct {
		AdvertiserID int     `sqlx:"advertiser_id"`
		DefaultCPM   float64 `sqlx:"default_cpm"`
		PartnerFee   float64 `sqlx:"partner_fee"`
		Spend        float64 `sqlx:"spend"`
	}
	type output struct{ Rows []row }

	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx,
		"CREATE TABLE advertiser_report(advertiser_id INTEGER, spend REAL)",
		"INSERT INTO advertiser_report VALUES(101,12.5)",
	); err != nil {
		t.Fatal(err)
	}
	groupable := true
	component := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Name: "AdvertiserReport"}, Routes: []*spec.Route{{Method: "GET", Path: "/advertiser"}},
		Settings: &spec.Settings{Cache: &spec.CacheSettings{
			Enabled: true, Name: "advertiser_report", Location: t.TempDir(), TTL: "1m",
			Warmup: &spec.CacheWarmupSettings{IndexColumn: "advertiser_id", IndexParameter: "AdvertiserID"},
		}},
		Parameters: []*spec.Parameter{
			{Name: "AdvertiserID", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "advertiser_id"}},
			{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
		RootView: &spec.View{Name: "advertiser_report", Groupable: &groupable, Source: &spec.ViewSource{SQL: `
SELECT
  advertiser_id,
  CAST(0 AS FLOAT64) AS default_cpm,
  CAST(0 AS FLOAT64) AS partner_fee,
  SUM(spend) AS spend
FROM advertiser_report
WHERE (:AdvertiserID=0 OR advertiser_id=:AdvertiserID)
GROUP BY 1,2,3`}},
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Rows"})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(output{}), Reader: reader}})
	if err != nil {
		t.Fatal(err)
	}
	target := dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "GET", Path: "/advertiser"}}
	if count, err := runtime.Warmup(ctx, target); err != nil || count != 1 {
		t.Fatalf("Warmup=%d,%v", count, err)
	}
	if err := db.ExecStatements(ctx, "DROP TABLE advertiser_report"); err != nil {
		t.Fatal(err)
	}
	actual, err := runtime.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Input: &input{AdvertiserID: 101}})
	if err != nil {
		t.Fatalf("warmup replay missed and attempted root SQL: %v", err)
	}
	rows := actual.(*output).Rows
	if len(rows) != 1 || rows[0].AdvertiserID != 101 || rows[0].Spend != 12.5 {
		t.Fatalf("rows=%+v", rows)
	}
}
