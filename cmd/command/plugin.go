package command

import (
	"context"
	"github.com/viant/afs/file"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/internal/codegen"
	"github.com/viant/datly/internal/plugin"
	"github.com/viant/pgo"
	"strings"
)

func (s *Service) BuildPlugin(ctx context.Context, plugin *options.Plugin) error {
	_ = s.fs.Create(ctx, plugin.DestURL, file.DefaultDirOsMode, true)
	request := &pgo.Options{
		Name:        plugin.Name,
		SourceURL:   plugin.Source,
		DestURL:     plugin.DestURL,
		Arch:        plugin.GoArch,
		Os:          plugin.GoOs,
		Version:     plugin.GoVersion,
		MainPath:    plugin.MainPath,
		BuildArgs:   plugin.BuildArgs,
		BuildMode:   "",
		Compression: "gzip",
		WithLogger:  true,
	}
	err := pgo.Build(request)
	return err
}

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
