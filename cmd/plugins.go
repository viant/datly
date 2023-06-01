package cmd

import (
	"bytes"
	"context"
	"fmt"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/pgo"
	"github.com/viant/xreflect"
	"go/format"
	"golang.org/x/mod/modfile"
	"path"
	"strings"
	"time"
)

const (
	importsFile       = "init.go"
	importsDirectory  = "dependency"
	checksumDirectory = "checksum"
	pluginDirectory   = "plugin"
	checksumFile      = "init.go"
	pluginFile        = "main.go"
	moduleCustomTypes = "github.com/viant/xdatly/types/custom"
	moduleCoreTypes   = "github.com/viant/xdatly/types/core"

	moduleCustomChecksum  = "github.com/viant/xdatly/types/custom/checksum"
	fileSideefectsImports = "xinit"
)

type bundleMetadata struct {
	url             string
	isCustomTypes   bool
	hasCoreTypesDep bool
	moduleName      string
}

func (m *bundleMetadata) shouldInitXDatlyModule() bool {
	if m == nil {
		return false
	}

	if m.isCustomTypes {
		return false
	}

	return m.hasCoreTypesDep
}

func (s *Builder) shouldGenPlugin(name string, types *xreflect.DirTypes) bool {
	methods := types.Methods(name)
	return len(methods) != 0
}

func (s *Builder) uploadPlugins() error {
	hasMod := map[string]string{}
	for _, pluginUrl := range s.plugins {
		if err := s.detectMod(pluginUrl, hasMod); err != nil {
			return err
		}
	}

	generated := map[string]bool{}
	for _, aPlugin := range s.plugins {
		modPath, ok := s.getModPath(aPlugin, hasMod)
		pluginPath := aPlugin.fileURL

		if ok && modPath != "" {
			if generated[modPath] {
				continue
			}

			pluginPath = modPath
			generated[modPath] = true
		}
		if modPath == "" {
			if err := s.genPlugin(aPlugin, pluginPath); err != nil {
				return err
			}
		}
	}

	s.config.PluginsURL = s.options.PluginsURL
	return nil
}

func (s *Builder) detectMod(pluginMeta *pluginGenDeta, modules map[string]string) error {
	dir := pluginMeta.URL
	location := s.dsqlDir()
	if s.options.ModuleURL != "" {
		location = s.options.ModuleURL
	}
	upFolders := map[string]bool{}
	for len(location) > 1 {
		upFolders[location] = true
		location = path.Dir(location)
	}

	for len(dir) > 1 {
		list, err := s.fs.List(context.Background(), dir)
		if err != nil {
			return err
		}

		var modURL string
		for _, object := range list {
			base := path.Base(object.URL())
			if base == "go.mod" {
				modURL = strings.ReplaceAll(object.URL(), "file://localhost", "")
			}
		}

		if upFolders[dir] {
			bundle, ok := s.isPluginBundle(dir)
			if ok {
				pluginMeta.fileURL = bundle.url
				modules[dir] = bundle.url
				pluginMeta.mainFile = path.Join("main.go")
			}

			return nil
		}

		if modURL != "" {
			modules[dir] = modURL
			return nil
		}

		dir = path.Dir(dir)
	}

	return nil
}

func (s *Builder) dsqlDir() string {
	if path.Ext(s.options.DSQLOutput) != "" {
		return path.Dir(s.options.DSQLOutput)
	}

	return s.options.DSQLOutput
}

func (s *Builder) getModPath(plugin *pluginGenDeta, mod map[string]string) (string, bool) {
	dir := path.Dir(plugin.URL)
	for len(dir) > 1 {
		if modPath, ok := mod[dir]; ok {
			return modPath, true
		}

		dir = path.Dir(dir)
	}

	return "", false
}

func (s *Builder) genPlugin(plugin *pluginGenDeta, aPath string) error {
	if s.options.IsPluginlessBuildMode() {
		return nil
	}

	var modBundle *bundleMetadata
	var sourceURL string
	var mainPath string
	if bundle, ok := s.isPluginBundle(aPath); ok {
		modBundle = bundle
		mainPath = path.Join(pluginDirectory, pluginFile)
		sourceURL = bundle.url
	} else {
		sourceURL = aPath
		mainPath = aPath
	}

	pluginName := path.Base(aPath)
	if ext := path.Ext(pluginName); ext != "" {
		pluginName = strings.Replace(pluginName, ext, ".so", 1)
	} else {
		pluginName = pluginName + ".so"
	}

	pluginPath := url.Path(s.options.PluginsURL)
	if index := strings.Index(pluginPath, "/Datly"); index != -1 {
		pluginPath = pluginPath[index:]
	}

	destURL := s.options.PluginDst
	if destURL == "" {
		destURL = path.Join(s.options.WriteLocation, pluginPath)
	}

	if err := s.buildBinary(sourceURL, destURL, pluginName, mainPath, "", true); err != nil {
		return err
	}

	if modBundle != nil {
		if err := s.updateLastGenPluginMeta(modBundle, time.Now()); err != nil {
			return err
		}
	}

	return nil
}

func (s *Builder) buildBinary(sourceURL string, destURL string, moduleName string, mainPath string, buildMode string, buildAsPlugin bool) error {
	arch := s.options.PluginArch
	os := s.options.PluginOS
	version := s.options.PluginGoVersion
	args := s.options.PluginArgs
	sources := []string{sourceURL}

	dependencies := s.options.PluginSrc

	if !buildAsPlugin {
		arch = s.options.ModuleArch
		os = s.options.ModuleOS
		version = s.options.ModuleGoVersion
		args = s.options.ModuleArgs
		dependencies = s.options.ModuleSrc
	}
	if len(dependencies) > 1 {
		sources = append(sources, dependencies[1:]...)
	}
	return pgo.Build(&pgo.Options{
		SourceURL:   sources,
		DestURL:     destURL,
		Name:        moduleName,
		Arch:        arch,
		Os:          os,
		Version:     version,
		MainPath:    mainPath,
		BuildArgs:   args,
		BuildMode:   buildMode,
		LdFlags:     s.options.ModuleLdFlags,
		Compression: "gzip",
		WithLogger:  true,
	})
}

func (s *Builder) updateLastGenPluginMeta(modBundle *bundleMetadata, now time.Time) error {
	if modBundle.shouldInitXDatlyModule() {
		_ = s.fs.Create(context.Background(), path.Join(modBundle.url, checksumDirectory), file.DefaultDirOsMode, true)
	}

	metaURL := path.Join(modBundle.url, checksumDirectory, checksumFile)
	content := fmt.Sprintf(`//Code generated by DATLY. DO NOT EDIT. GeneratedTime will be updated whenever new plugin was generated.
//Please use GeneratedTime to Register types. It will help to keep types synchronized when using plugins.

package %v

import "time"

var GeneratedTime, _ = time.Parse(time.RFC3339, "%v")
`, checksumDirectory, now.Format(time.RFC3339))

	formatted, err := format.Source([]byte(content))
	if err != nil {
		return err
	}

	return s.fs.Upload(context.Background(), metaURL, file.DefaultFileOsMode, bytes.NewReader(formatted))
}

func (s *Builder) isPluginBundle(URL string) (*bundleMetadata, bool) {
	if boundleURL, ok := s.bundles[URL]; ok {
		return boundleURL, boundleURL != nil
	}

	bundleURL, ok := s.checkIfIsPluginBundle(URL)
	s.bundles[URL] = bundleURL
	return bundleURL, ok
}

func (s *Builder) checkIfIsPluginBundle(URL string) (*bundleMetadata, bool) {
	for len(URL) > 1 {
		if ext := path.Ext(URL); ext != "" {
			URL = strings.Replace(URL, ext, "", 1)
		}

		list, _ := s.fs.List(context.Background(), URL)

		for _, aFile := range list {
			fileURL := aFile.URL()
			if path.Base(fileURL) != "go.mod" {
				continue
			}

			fileContent, err := s.fs.DownloadWithURL(context.Background(), fileURL)
			if err != nil {
				return nil, false
			}

			parse, err := modfile.Parse("go.mod", fileContent, nil)
			if err != nil {
				return nil, false
			}

			hasCoreDependency := s.hasDependency(parse, moduleCoreTypes)
			isCustomTypes := parse.Module.Mod.Path == moduleCustomTypes
			if (isCustomTypes || hasCoreDependency) && (parse.Module.Mod.Path != "github.com/viant/datly") {
				replace := strings.Replace(path.Dir(fileURL), "file://localhost", "", 1)
				replace = strings.Replace(replace, "file:/localhost", "", 1)
				return &bundleMetadata{
					moduleName:      parse.Module.Mod.Path,
					url:             replace,
					hasCoreTypesDep: hasCoreDependency,
					isCustomTypes:   isCustomTypes,
				}, true
			}

			return nil, false
		}

		URL = path.Dir(URL)
	}

	return nil, false
}

func (s *Builder) hasDependency(mod *modfile.File, moduleName string) bool {
	for _, require := range mod.Require {
		if require.Mod.Path == moduleName {
			return true
		}
	}

	return false
}

func (s *Builder) ensurePlugins(bundle *bundleMetadata) error {
	background := context.Background()
	pluginDir := path.Join(bundle.url, pluginDirectory)

	if ok, _ := s.fs.Exists(background, pluginDir); ok {
		return nil
	}

	if err := s.fs.Create(background, pluginDir, file.DefaultDirOsMode, true); err != nil {
		return err
	}

	source, err := format.Source([]byte(fmt.Sprintf(`
		package main

		import _ "%v"
`, bundle.moduleName)))

	if err != nil {
		return err
	}

	return s.fs.Upload(background, path.Join(pluginDir, pluginFile), file.DefaultFileOsMode, bytes.NewReader(source))
}
