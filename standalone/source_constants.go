package standalone

import (
	"context"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/typecatalog"
)

// validateConstants performs source/typed registration checks before opening
// connectors. No generated artifacts are written and no SQL is sent to a DB.
func (s *source) validateConstants(ctx context.Context, types *typecatalog.Catalog) error {
	if s.config.GoBootstrap == nil {
		return nil
	}
	project, err := (&transcribe.Discovery{Const: s.config.Const, Workspace: s.Workspace, BaseDir: s.config.BaseDir, ModuleDirs: s.config.ModuleDirs, Include: s.config.GoBootstrap.Packages, Exclude: s.config.GoBootstrap.Exclude, Connector: s.config.Connector, Types: types, Registry: s.registry}).Compile(ctx)
	if err != nil {
		return err
	}
	builder, err := bootstrap.NewArtifactBuilder(s.registry)
	if err != nil {
		return err
	}
	components := &sourceComponent{source: s}
	for _, result := range project.Components {
		if result.Component.Static != nil {
			continue
		}
		input, err := components.artifactInput(result)
		if err != nil {
			return err
		}
		if _, err = builder.Build(input); err != nil {
			return err
		}
	}
	return nil
}
