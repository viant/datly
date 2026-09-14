package report

import (
	"fmt"

	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

type ProjectConfig struct {
	// Registry supplies compiled package exports; CompileArtifacts snapshots it
	// once for the whole unpublished component generation.
	Registry  *x.Registry
	Types     *typecatalog.Catalog
	Authority typecatalog.Authority
}

// ProjectCompiler derives cube components from base components whose exact
// input contracts have already been compiled.
type ProjectCompiler struct {
	config ProjectConfig
}

func NewProjectCompiler(config ProjectConfig) *ProjectCompiler {
	if config.Authority == "" {
		config.Authority = typecatalog.PackageAuthority
	}
	return &ProjectCompiler{config: config}
}

func (c *ProjectCompiler) Compile(sources []Source) (*Project, error) {
	if c == nil {
		return nil, fmt.Errorf("report project compiler is required")
	}
	return newReportDeriver(c).compile(sources)
}
