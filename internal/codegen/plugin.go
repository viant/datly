package codegen

import (
	_ "embed"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/plugin"
	"strings"
	"time"
)

type Plugin struct{}

//go:embed tmpl/plugin/dependency.gox
var dependencyGoTemplate string

func (p *Plugin) GenerateDependency(info *plugin.Info) string {
	imports := p.getImports(info)
	return strings.Replace(dependencyGoTemplate, "$Imports", imports, 1)
}

func (p *Plugin) getImports(info *plugin.Info) string {
	imports := inference.NewImports()
	if len(info.CustomTypesPackages) == 0 && len(info.CustomTypesPackages) == 0 {
		return ""
	}
	for _, pkg := range info.CustomTypesPackages {
		imports.AddPackage(pkg.ID)
	}
	for _, pkg := range info.CustomCodecPackages {
		imports.AddPackage(pkg.ID)
	}
	return imports.DefaultPackageImports()
}

//go:embed tmpl/plugin/checksum.gox
var checksumGoTemplate string

func (p *Plugin) GenerateChecksum(info *plugin.Info) string {
	return strings.Replace(checksumGoTemplate, "$Time", time.Now().Format(time.RFC3339), 1)
}

//go:embed tmpl/plugin/main.gox
var mainGoTemplate string

func (p *Plugin) GeneratePlugin(info *plugin.Info) string {
	imports := inference.NewImports()
	imports.AddPackage(info.DependencyPkg())
	return strings.Replace(mainGoTemplate, "$Imports", imports.DefaultPackageImports(), 1)
}

func NewPlugin() *Plugin {
	return &Plugin{}
}
