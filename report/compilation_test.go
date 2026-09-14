package report

import (
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestCompileArtifactsBuildsIsolatedBaseAndReportSet(t *testing.T) {
	authority := typecatalog.NewCatalog()
	compilation, err := NewProjectCompiler(ProjectConfig{Types: authority}).CompileArtifacts([]bootstrap.ArtifactInput{
		reportArtifactInput(t, &spec.ReportSettings{Enabled: true}),
	})
	if err != nil {
		t.Fatalf("CompileArtifacts() error = %v", err)
	}
	artifacts := compilation.Artifacts()
	if len(artifacts) != 2 || artifacts[0].IsReport() || !artifacts[1].IsReport() {
		t.Fatalf("compiled report flags = %v", reportFlags(artifacts))
	}
	derived := artifacts[1]
	if derived.ReaderCompilation() != nil || derived.Component().Settings.Report != nil {
		t.Fatalf("derived report retained reader recursion: %+v", derived.Component())
	}
	typeKey := derived.Component().Settings.InputType
	if _, ok, resolveErr := authority.Resolve(typecatalog.PackageAuthority, typeKey); resolveErr != nil || ok {
		t.Fatalf("source authority catalog was mutated: ok=%v err=%v", ok, resolveErr)
	}
	compiledTypes, err := compilation.Types()
	if err != nil {
		t.Fatal(err)
	}
	resolved, ok, err := compiledTypes.Resolve(typecatalog.PackageAuthority, typeKey)
	if err != nil || !ok || resolved.Type != derived.InputType() {
		t.Fatalf("compiled report type = %+v, ok=%v err=%v", resolved, ok, err)
	}
	if err = compiledTypes.Register(typecatalog.TypeOriginGenerated, &x.Type{
		Type: reflect.TypeOf(struct{}{}), PkgPath: "example.com/detached", Name: "Detached",
	}); err != nil {
		t.Fatal(err)
	}
	fresh, err := compilation.Types()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err = fresh.Resolve(typecatalog.PackageAuthority, "example.com/detached.Detached"); err != nil || ok {
		t.Fatalf("Types() snapshot mutation reached compilation: ok=%v err=%v", ok, err)
	}
	detached := derived.Component()
	detached.Routes = nil
	detached.Settings.InputType = "Changed"
	if actual := derived.Component(); len(actual.Routes) != 1 || actual.Settings.InputType != typeKey {
		t.Fatalf("detached component mutation reached compilation: %+v", actual)
	}
}

func TestCompileArtifactsFailureDoesNotMutateAuthorityCatalog(t *testing.T) {
	broken := &spec.Component{
		Key:      spec.Key{Kind: spec.KindComponent, Scope: "example.com/acme/reporting", Name: "ZBroken"},
		Settings: &spec.Settings{InputType: "BrokenInput", OutputType: "BrokenOutput"},
	}
	authority := typecatalog.NewCatalog()
	compilation, err := NewProjectCompiler(ProjectConfig{Types: authority}).CompileArtifacts([]bootstrap.ArtifactInput{
		reportArtifactInput(t, &spec.ReportSettings{Enabled: true}),
		{Component: broken, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeOf(struct{}{})},
	})
	if err == nil || compilation != nil {
		t.Fatalf("CompileArtifacts() = %+v, %v", compilation, err)
	}
	if _, ok, resolveErr := authority.Resolve(typecatalog.PackageAuthority, "example.com/acme/reporting.SpendCubeInput"); resolveErr != nil || ok {
		t.Fatalf("failed compilation mutated authority catalog: ok=%v err=%v", ok, resolveErr)
	}
}

func reportArtifactInput(t *testing.T, settings *spec.ReportSettings) bootstrap.ArtifactInput {
	t.Helper()
	source := reportSource(t, settings)
	component := source.Component.Clone()
	component.RootView.Relations = nil
	return bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(reportSourceInput{}),
		OutputType: reflect.TypeOf(reportSourceOutput{}), DirectViewField: "Rows",
	}
}

func reportFlags(artifacts []*ComponentArtifact) []bool {
	result := make([]bool, len(artifacts))
	for index, artifact := range artifacts {
		result[index] = artifact.IsReport()
	}
	return result
}
