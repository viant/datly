package link

import (
	"testing"

	"github.com/viant/datly/spec"
)

func TestSettingsLoaderDropsGeneratedCubeCompanionMetadata(t *testing.T) {
	base := &spec.Settings{Report: &spec.ReportSettings{Enabled: true, LinkedFacade: true}}
	for _, authored := range []*spec.Settings{nil, {}, {Report: &spec.ReportSettings{Enabled: true}}} {
		actual := (&settingsLoader{base: base, authored: authored}).Load()
		if actual.Report == nil || !actual.Report.Enabled || actual.Report.LinkedFacade {
			t.Fatalf("DQL inherited generated companion metadata: %+v", actual.Report)
		}
		if !base.Report.LinkedFacade {
			t.Fatal("settings loader mutated the linked publication")
		}
	}
}

func TestSettingsLoaderPreservesExplicitDQLPolicyOverrides(t *testing.T) {
	ignoreEmpty := true
	base := &spec.Settings{
		IgnoreEmptyQueryParameters: &ignoreEmpty,
		ResponseCompression:        &spec.ResponseCompression{Encoding: "gzip", MinSizeBytes: 1024},
		ComponentCallPolicy:        "buffered",
		SequenceStrategy:           "package",
	}
	falseValue := false
	for _, authored := range []*spec.Settings{
		{IgnoreEmptyQueryParameters: &falseValue, ResponseCompression: &spec.ResponseCompression{Encoding: "gzip", MinSizeBytes: 2048}, ComponentCallPolicy: "imperative", SequenceStrategy: "dql", IndependentChildTransactions: true},
		{IgnoreEmptyQueryParameters: &ignoreEmpty},
	} {
		actual := (&settingsLoader{base: base, authored: authored}).Load()
		if actual.IgnoreEmptyQueryParameters == nil || *actual.IgnoreEmptyQueryParameters != *authored.IgnoreEmptyQueryParameters {
			t.Fatalf("DQL query policy lost: %+v", actual)
		}
		if authored.ResponseCompression != nil {
			if actual.ResponseCompression == nil || actual.ResponseCompression.MinSizeBytes != 2048 || actual.ComponentCallPolicy != "imperative" || actual.SequenceStrategy != "dql" || !actual.IndependentChildTransactions {
				t.Fatalf("DQL policies lost: %+v", actual)
			}
			actual.ResponseCompression.MinSizeBytes = 1
			if authored.ResponseCompression.MinSizeBytes != 2048 {
				t.Fatal("merged compression aliases authored settings")
			}
		} else if actual.ResponseCompression.MinSizeBytes != 1024 || actual.ComponentCallPolicy != "buffered" || actual.SequenceStrategy != "package" {
			t.Fatalf("package policies were not inherited: %+v", actual)
		}
	}
	if !*base.IgnoreEmptyQueryParameters || base.ResponseCompression.MinSizeBytes != 1024 {
		t.Fatal("merged settings mutated the package policy")
	}
}

func TestSettingsLoaderOverlaysNestedSettingsWithoutAliasing(t *testing.T) {
	base := &spec.Settings{
		Generation: &spec.GenerationSettings{Template: "reader", ViewFile: "package.go", InputFile: "package_input.go", FilePrefix: "base_", SupportFiles: map[string]string{"frames": "base_frames.go", "entities": "base_entities.go"}},
		Report: &spec.ReportSettings{Enabled: true, LinkedInputType: "PackageInput", InputLayout: &spec.ReportInputLayout{
			Dimensions: "PackageDimensions", Measures: "PackageMeasures",
		}},
		Cache: &spec.CacheSettings{Enabled: true, Name: "package-cache", Warmup: &spec.CacheWarmupSettings{
			Cases: []*spec.CacheWarmupCase{{Set: []*spec.CacheWarmupParam{{Name: "tenant", Values: []string{"1"}}}}},
		}},
		Const: map[string]string{"shared": "package", "package": "only"},
	}
	authored := &spec.Settings{
		Generation: &spec.GenerationSettings{Template: "patch", ViewFile: "dql.go", FilePrefix: "authored_", HandlerFile: "execute.go", LifecycleFile: "hooks.go", MutationFile: "policy.go", ResourcesFile: "sql.go", LinksFile: "links.go", TemplateFile: "main.velty", SupportFiles: map[string]string{"frames": "state.go"}},
		Report: &spec.ReportSettings{Enabled: true, InputLayout: &spec.ReportInputLayout{
			Dimensions: "DQLDimensions",
		}},
		Cache: &spec.CacheSettings{Enabled: false},
		Const: map[string]string{"shared": "dql"},
	}
	actual := (&settingsLoader{base: base, authored: authored}).Load()
	if actual.Report == nil || !actual.Report.Enabled || actual.Report.LinkedInputType != "" || actual.Report.InputLayout == nil ||
		actual.Report.InputLayout.Dimensions != "DQLDimensions" || actual.Report.InputLayout.Measures != "PackageMeasures" ||
		actual.Cache == nil || actual.Cache.Enabled || actual.Cache.Name != "" || actual.Cache.Warmup != nil ||
		actual.Generation == nil || actual.Generation.Template != "patch" || actual.Generation.ViewFile != "dql.go" || actual.Generation.InputFile != "package_input.go" ||
		actual.Const["shared"] != "dql" || actual.Const["package"] != "only" {
		t.Fatalf("settings = %+v", actual)
	}
	actual.Const["package"] = "changed"
	for role, want := range map[string]string{"handler": "execute.go", "lifecycle": "hooks.go", "mutation": "policy.go", "resources": "sql.go", "links": "links.go", "template": "main.velty", "frames": "state.go", "entities": "base_entities.go"} {
		if got := actual.Generation.File(role, "fallback.go"); got != want {
			t.Fatalf("merged %s=%s", role, got)
		}
	}
	if actual.Generation.FilePrefix != "authored_" {
		t.Fatal("authored prefix lost")
	}
	actual.Generation.SupportFiles["entities"] = "changed.go"
	if base.Generation.SupportFiles["entities"] != "base_entities.go" || authored.Generation.SupportFiles["frames"] != "state.go" {
		t.Fatal("generation map aliases source")
	}
	actual.Generation.InputFile = "changed.go"
	actual.Report.InputLayout.Measures = "changed"
	if base.Cache.Warmup.Cases[0].Set[0].Values[0] != "1" || base.Const["package"] != "only" {
		t.Fatal("loaded settings alias package settings")
	}
	if base.Report.InputLayout.Measures != "PackageMeasures" {
		t.Fatal("loaded report input layout aliases package settings")
	}
	if base.Generation.InputFile != "package_input.go" {
		t.Fatal("loaded generation settings alias package settings")
	}
}

func TestSettingsLoaderAuthoredEmptyReportInputSelectsGeneratedType(t *testing.T) {
	actual := (&settingsLoader{
		base:     &spec.Settings{Report: &spec.ReportSettings{Enabled: true, LinkedInputType: "PackageInput"}},
		authored: &spec.Settings{Report: &spec.ReportSettings{Enabled: true}},
	}).Load()
	if actual.Report == nil || actual.Report.LinkedInputType != "" {
		t.Fatalf("report settings = %+v, want generated input selection", actual.Report)
	}
}

func TestSettingsLoaderComposeOnlyOverlayPreservesPackageReport(t *testing.T) {
	actual := (&settingsLoader{
		base:     &spec.Settings{Report: &spec.ReportSettings{Enabled: true}},
		authored: &spec.Settings{Report: &spec.ReportSettings{Compose: &spec.CubeComposeSettings{Enabled: true}}},
	}).Load()
	if actual.Report == nil || !actual.Report.Enabled || actual.Report.Compose == nil || !actual.Report.Compose.Enabled {
		t.Fatalf("report settings=%+v", actual.Report)
	}
}

func TestSettingsLoaderOverlaysConstantsByCanonicalName(t *testing.T) {
	base := &spec.Settings{Const: map[string]string{"Vendor": "package", "PackageOnly": "kept"}}
	authored := &spec.Settings{Const: map[string]string{"vendor": "dql"}}
	actual := (&settingsLoader{base: base, authored: authored}).Load()
	if len(actual.Const) != 2 || actual.Const["vendor"] != "dql" || actual.Const["PackageOnly"] != "kept" {
		t.Fatalf("constants = %+v", actual.Const)
	}
	if _, ok := actual.Const["Vendor"]; ok {
		t.Fatalf("package spelling survived authored override: %+v", actual.Const)
	}
	if base.Const["Vendor"] != "package" {
		t.Fatalf("base constants mutated: %+v", base.Const)
	}
}

func TestComponentLoaderRequestBodyModeAuthority(t *testing.T) {
	for _, tc := range []struct {
		name, path, authored, base, want string
		bad                              bool
	}{
		{"inherit", "/views", "", "on_demand", "on_demand", false},
		{"same", "/views", "on_demand", "on_demand", "on_demand", false},
		{"conflict", "/views", "eager", "on_demand", "", true},
		{"different route", "/other", "", "on_demand", "", false},
		{"invalid", "/views", "invalid", "", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := &spec.Route{Method: "DELETE", Path: "/views", RequestBodyMode: tc.base}
			got, err := (&Loader{}).mergeRoutes([]*spec.Route{base}, []*spec.Route{{Method: "DELETE", Path: tc.path, RequestBodyMode: tc.authored}})
			if tc.bad {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got[0].RequestBodyMode != tc.want {
				t.Fatalf("mode: %q", got[0].RequestBodyMode)
			}
			got[0].RequestBodyMode = "eager"
			if base.RequestBodyMode != tc.base {
				t.Fatal("modified package authority")
			}
		})
	}
}

func TestMergeGenerationPreservesWriterOmission(t *testing.T) {
	for _, inherited := range []bool{false, true} {
		base := &spec.GenerationSettings{WriterOmitEmpty: inherited, ViewFile: "rows.go"}
		got := (&settingsLoader{}).mergeGeneration(base, &spec.GenerationSettings{WriterOmitEmpty: true})
		if !got.WriterOmitEmpty || got.ViewFile != "rows.go" || base.WriterOmitEmpty != inherited {
			t.Fatalf("merge lost omission or mutated base: %+v", got)
		}
	}
	if got := (&settingsLoader{}).mergeGeneration(nil, &spec.GenerationSettings{WriterOmitEmpty: true}); !got.WriterOmitEmpty {
		t.Fatal("authored omission lost without base")
	}
}

func TestMergeGenerationPreservesSQLDestinations(t *testing.T) {
	base := &spec.GenerationSettings{SQLFiles: map[string]string{"records": "old.sql", "children": "children.sql"}}
	authored := &spec.GenerationSettings{SQLFiles: map[string]string{"records": "queries/records.sql"}}
	got := (&settingsLoader{}).mergeGeneration(base, authored)
	if got.SQLFile("records", "") != "queries/records.sql" || got.SQLFile("children", "") != "children.sql" {
		t.Fatalf("lost SQL destinations: %+v", got.SQLFiles)
	}
	got.SQLFiles["records"] = "changed.sql"
	if base.SQLFiles["records"] != "old.sql" || authored.SQLFiles["records"] != "queries/records.sql" {
		t.Fatal("merged SQL destinations alias source settings")
	}
	fresh := (&settingsLoader{}).mergeGeneration(nil, authored)
	if fresh == nil || fresh.SQLFile("records", "") != "queries/records.sql" {
		t.Fatal("standalone SQL destination discarded")
	}
}
