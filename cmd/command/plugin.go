package command

import (
	"bytes"
	"compress/gzip"
	"context"
	"debug/buildinfo"
	"fmt"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/internal/codegen"
	"github.com/viant/datly/internal/plugin"
	"github.com/viant/pgo"
	"github.com/viant/pgo/manager"
	"io"
	"os"
	"path"
	"runtime/debug"
	"strconv"
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

func (s *Service) loadPlugin(ctx context.Context, opts *options.Options) (err error) {
	if !opts.Repository().LoadPlugin {
		return
	}
	moduleLocation := opts.Rule().ModuleLocation
	goMod := path.Join(moduleLocation, "go.mod")
	if ok, _ := s.fs.Exists(ctx, goMod); !ok {
		return nil
	}
	flags := getGcFlags()
	repo := opts.Repository()
	destURL := url.Join(repo.ProjectURL, ".build/plugin")
	_ = s.fs.Delete(ctx, destURL)
	_ = s.fs.Create(ctx, destURL, file.DefaultDirOsMode, true)

	aPlugin := &options.Plugin{GoBuild: options.GoBuild{Module: moduleLocation,
		DestURL: destURL,
		Source:  []string{moduleLocation},
		BuildArgs: []string{
			flags,
		},
	}}
	if err = aPlugin.Init(); err != nil {
		return err
	}
	if err := s.BuildPlugin(ctx, aPlugin); err != nil {
		return err
	}
	pManager := manager.New(0)
	pluginInfo := s.getPluginInfoURL(ctx, destURL)
	_, _, err = pManager.OpenWithInfoURL(ctx, pluginInfo)
	if err != nil {
		if rErr := s.reportPluginIssue(ctx, destURL); rErr != nil {
			return rErr
		}
	}
	return err
}

func (s *Service) reportPluginIssue(ctx context.Context, destURL string) error {
	plugin, err := s.getPluginBinary(ctx, destURL)
	if err != nil {
		return err
	}
	pluginInfo, err := buildinfo.Read(bytes.NewReader(plugin))
	if err != nil {
		return err
	}
	runtimeInfo, err := getRuntimeBuildInfo()
	if err != nil {
		return err
	}

	runtimeDep := map[string]*debug.Module{}
	for _, dep := range runtimeInfo.Deps {
		runtimeDep[dep.Path] = dep
	}
	for _, candidate := range pluginInfo.Deps {
		rtDep, ok := runtimeDep[candidate.Path]
		if !ok {
			continue
		}
		if rtDep.Version != candidate.Version || rtDep.Sum != candidate.Sum {
			fmt.Printf("dependency difference: %v %v <-> %v", candidate.Path, rtDep.Version, candidate.Version)
		}
	}
	return nil
}

func (s *Service) getPluginInfoURL(ctx context.Context, destURL string) string {
	objects, _ := s.fs.List(ctx, destURL)
	for _, object := range objects {
		if path.Ext(object.Name()) == ".pinf" {
			return object.URL()
		}
	}
	return ""
}

func (s *Service) getPluginBinary(ctx context.Context, destURL string) ([]byte, error) {
	objects, _ := s.fs.List(ctx, destURL)
	for _, object := range objects {
		if path.Ext(object.Name()) == ".gz" {
			data, err := s.fs.Download(ctx, object)
			if err != nil {
				return nil, err
			}
			reader, err := gzip.NewReader(bytes.NewReader(data))
			if err != nil {
				return nil, err
			}
			dest := bytes.Buffer{}
			io.Copy(&dest, reader)
			reader.Close()
			return dest.Bytes(), nil
		}
		if path.Ext(object.Name()) == "so" {
			return s.fs.Download(ctx, object)
		}

	}
	return nil, fmt.Errorf("not found")
}

func getGcFlags() string {
	build, err := getRuntimeBuildInfo()
	if err != nil {
		return ""
	}
	for _, setting := range build.Settings {
		if setting.Key == "-gcflags" {
			return setting.Key + " " + strconv.Quote(setting.Value)
		}
	}
	return ""
}

func getRuntimeBuildInfo() (*buildinfo.BuildInfo, error) {
	fileName, err := os.Executable()
	if err != nil {
		return nil, err
	}
	return buildinfo.ReadFile(fileName)
}
