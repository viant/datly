package command

import (
	"context"
	"github.com/viant/afs/file"
	"github.com/viant/datly/internal/codegen"
	"github.com/viant/datly/internal/plugin"
	"strings"
)

func (s *Service) EnsurePluginArtifacts(ctx context.Context, info *plugin.Info) error {
	switch info.IntegrationMode {
	case plugin.ModeExtension, plugin.ModeCustomTypeModule:
	default:
		return nil
	}
	codeGenPlugin := codegen.NewPlugin()
	dep := codeGenPlugin.GenerateDependency(info)
	if err := s.fs.Upload(ctx, info.DependencyURL(), file.DefaultFileOsMode, strings.NewReader(dep)); err != nil {
		return err
	}
	checksum := codeGenPlugin.GenerateChecksum(info)
	if err := s.fs.Upload(ctx, info.ChecksumURL(), file.DefaultFileOsMode, strings.NewReader(checksum)); err != nil {
		return err
	}
	main := codeGenPlugin.GeneratePlugin(info)
	return s.fs.Upload(ctx, info.PluginURL(), file.DefaultFileOsMode, strings.NewReader(main))
}
