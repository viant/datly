package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/datly/plugins"
	"github.com/viant/xreflect"
	"go/ast"
	"go/format"
	"golang.org/x/mod/modfile"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	importsFile = "imports_enc.go"
)

type (
	renamer struct {
		originalPath string
		replacedPath string
		index        *processed
		fs           afs.Service
	}

	processed struct {
		sync.Mutex
		index map[string]bool
	}
)

func (p *processed) processed(URL string) bool {
	p.Lock()
	defer p.Unlock()

	if p.index[URL] {
		return true
	}

	p.index[URL] = true
	return false
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

		if err := s.genPlugin(aPlugin, pluginPath); err != nil {
			return err
		}
	}

	return nil
}

func (s *Builder) detectMod(pluginMeta *pluginGenDeta, modules map[string]string) error {
	dir := pluginMeta.URL
	location := path.Dir(s.options.Location)

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
			boundlePath, ok := s.IsPluginBundle(dir)
			if ok {
				pluginMeta.fileURL = boundlePath
				modules[dir] = boundlePath
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
	now := TimeNow()
	nowInNano := int(now.Unix())
	bundleURL, isBundle := s.IsPluginBundle(aPath)
	suffix := strconv.Itoa(nowInNano)
	name := path.Base(aPath)

	pluginPath := path.Join(os.TempDir(), "plugins", suffix, name)
	if isBundle {
		base := path.Base(aPath)
		if err := s.copyAndRenameModule(nowInNano, bundleURL, path.Dir(aPath), pluginPath); err != nil {
			return err
		}

		pluginPath = path.Join(pluginPath, base)
	} else {
		if err := s.fs.Copy(context.Background(), aPath, pluginPath); err != nil {
			return err
		}
	}

	pluginName := name
	if ext := path.Ext(name); ext != "" {
		pluginName = pluginName[:len(pluginName)-len(ext)] + ".so"
	}

	pluginDst := path.Join(path.Dir(pluginPath), pluginName)
	base := path.Base(pluginDst)
	if ext := path.Ext(base); ext != "" {
		base = strings.Replace(pluginDst, ext, ".so", 1)
	} else if isBundle {
		pluginDst = path.Join(pluginPath, "main.so")
	} else {
		pluginDst = path.Join(pluginDst, base+".so")
	}

	dir, args := s.pluginArgs(pluginDst, pluginPath, plugin, isBundle)
	command := exec.Command("go", args...)
	if path.Ext(dir) != "" {
		command.Dir = path.Dir(dir)
	} else {
		command.Dir = dir
	}

	//command := exec.Command("bash", `-c`, `/usr/local/bin/go build -buildmode=plugin -o /tmp/main.so`)
	fmt.Printf("Generating plugin | %v\n", command.String())
	//command.Dir = "/var/folders/mw/5wzs15hd53sgm1vzs4tryg1w0000gn/T/plugins/1675737394/custom/custom"
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("couldn't generate plugin due to the: %w\n | console output: %s", err, output)
	}

	if err = s.fs.Copy(context.Background(), pluginDst, s.options.DependencyURL); err != nil {
		return err
	}

	if err = s.genPluginMetadata(pluginDst, now); err != nil {
		return err
	}

	if isBundle {
		return s.updateLastGenPluginMeta(bundleURL, now)
	}

	return nil
}

func (s *Builder) copyAndRenameModule(now int, bundleURL string, aPath string, pluginPath string) error {
	aRenamer := &renamer{
		index:        &processed{index: map[string]bool{}},
		originalPath: aPath,
		replacedPath: pluginPath,
		fs:           s.fs,
	}

	if err := s.fs.Copy(context.Background(), aPath, pluginPath); err != nil {
		return err
	}

	modContent, err := s.fs.DownloadWithURL(context.Background(), path.Join(bundleURL, "go.mod"))
	if err != nil {
		return err
	}

	aMod, err := modfile.Parse("go.mod", modContent, nil)
	if err != nil {
		return err
	}

	oldModule := aMod.Module.Mod.Path
	newModule := oldModule + "_t" + strconv.Itoa(now)
	list, err := s.fs.List(context.Background(), pluginPath)
	if err != nil {
		return err
	}

	return aRenamer.renameModule(pluginPath, list, oldModule, newModule)
}

func (r *renamer) renameModule(parentPath string, list []storage.Object, oldName, newName string) error {
	wg := &sync.WaitGroup{}
	var resultErr error
	for _, object := range list {
		if r.index.processed(object.URL()) {
			continue
		}

		if object.URL() == parentPath {
			continue
		}

		wg.Add(1)
		go func(wg *sync.WaitGroup, object storage.Object) {
			defer wg.Done()
			objURL := object.URL()
			ext := path.Ext(objURL)
			switch ext {
			case "":
				objects, err := r.fs.List(context.Background(), objURL)
				if err != nil {
					resultErr = err
					return
				}

				if err = r.renameModule(object.URL(), objects, oldName, newName); err != nil {
					resultErr = err
					return
				}

			case ".go", ".mod", ".sum":
				objContent, err := r.fs.Download(context.Background(), object)
				if ext == ".mod" {
					parse, err := modfile.Parse("go.mod", objContent, nil)
					if err != nil {
						resultErr = err
						return
					}

					for _, replace := range parse.Replace {
						expand, ok := r.expandRelativePath(path.Dir(object.URL()), replace.New.Path)
						if ok {
							objContent = bytes.ReplaceAll(objContent, []byte(replace.New.Path), []byte(expand))
						}
					}
				}

				if err != nil {
					resultErr = err
					return
				}

				newContent := bytes.ReplaceAll(objContent, []byte(oldName), []byte(newName))
				if len(objContent) != len(newContent) {
					if err = r.fs.Upload(context.Background(), object.URL(), file.DefaultFileOsMode, bytes.NewReader(newContent)); err != nil {
						resultErr = err
					}
				}
			}

		}(wg, object)

	}

	wg.Wait()
	return resultErr
}

func (s *Builder) pluginArgs(pluginDst string, pluginPath string, plugin *pluginGenDeta, bundle bool) (string, []string) {
	args := []string{
		"build",
		"-buildmode=plugin",
	}

	for _, pluginArg := range s.options.PluginArgs {
		argsReg := regexp.MustCompile(`([-a-zA-Z]+)|(".*?[^\\]")|("")`)
		pluginArgs := argsReg.FindAllString(pluginArg, -1)

		for i, arg := range pluginArgs {
			if !strings.HasPrefix(arg, `"`) || !strings.HasSuffix(arg, `"`) {
				continue
			}

			var err error
			pluginArgs[i], err = strconv.Unquote(arg)
			if err != nil {
				pluginArgs[i] = arg
			}
		}

		args = append(args, pluginArgs...)
	}

	if plugin.mainFile != "" {
		pluginPath = path.Join(pluginPath, plugin.mainFile)
	}

	args = append(args,
		"-o",
		pluginDst,
	)

	if !bundle {
		args = append(args, path.Base(pluginPath))
	}

	return pluginPath, args
}

func (s *Builder) readPackageNameValue(plugin *pluginGenDeta) string {
	packageValue, err := plugin.filesMeta.Value(plugins.PackageName)
	if err != nil {
		return ""
	}

	lit, ok := packageValue.(*ast.BasicLit)
	if !ok {
		return ""
	}

	result, err := strconv.Unquote(lit.Value)
	if err != nil {
		return lit.Value
	}

	return result
}

func (s *Builder) genPluginMetadata(pluginPath string, generatedTime time.Time) error {
	pluginMeta := &plugins.Metadata{
		CreationTime: generatedTime,
	}

	marshal, err := json.Marshal(pluginMeta)
	if err != nil {
		return err
	}

	return s.fs.Upload(context.Background(), url.Join(s.options.DependencyURL, path.Base(pluginPath)+".meta"), file.DefaultFileOsMode, bytes.NewReader(marshal))
}

func (s *Builder) updateLastGenPluginMeta(URL string, now time.Time) error {

	metaURL := path.Join(URL, "generated", "meta_enc.go")
	content := fmt.Sprintf(`//Code generated by DATLY. DO NOT EDIT. GeneratedTime will be updated whenever new plugin was generated.
//Please use GeneratedTime to Register types. It will help to keep types synchronized when using plugins.

package generated

import "time"

var GeneratedTime, _ = time.Parse(time.RFC3339, "%v")
`, now.Format(time.RFC3339))

	formatted, err := format.Source([]byte(content))
	if err != nil {
		return err
	}

	return s.fs.Upload(context.Background(), metaURL, file.DefaultFileOsMode, bytes.NewReader(formatted))
}

func (s *Builder) IsPluginBundle(URL string) (string, bool) {
	if boundleURL, ok := s.bundles[URL]; ok {
		return boundleURL, boundleURL != ""
	}

	bundleURL, ok := s.isPluginBundle(URL)
	s.bundles[URL] = bundleURL
	return bundleURL, ok
}

func (s *Builder) isPluginBundle(URL string) (string, bool) {
	for len(URL) > 1 {
		list, err := s.fs.List(context.Background(), URL)
		if err != nil {
			return "", false
		}

		for _, aFile := range list {
			fileURL := aFile.URL()
			if path.Base(fileURL) != "go.mod" {
				continue
			}

			fileContent, err := s.fs.DownloadWithURL(context.Background(), fileURL)
			if err != nil {
				return "", false
			}

			parse, err := modfile.Parse("go.mod", fileContent, nil)
			if err != nil {
				return "", false
			}

			if parse.Module.Mod.Path == "github.com/viant/datly/xregistry/types/custom" {
				replace := strings.Replace(path.Dir(fileURL), "file://localhost", "", 1)
				replace = strings.Replace(replace, "file:/localhost", "", 1)
				return replace, true
			}

			return "", false
		}

		URL = path.Dir(URL)
	}

	return "", false
}

func (r *renamer) expandRelativePath(currentDirectory string, aPath string) (string, bool) {
	currentDirectory = strings.ReplaceAll(currentDirectory, "file://localhost", "")
	currentDirectory = strings.ReplaceAll(currentDirectory, "file:/localhost", "")
	currentDirectory = strings.Replace(currentDirectory, r.replacedPath, r.originalPath, 1)

	segments := strings.Split(aPath, "/")
	for i, segment := range segments {
		if !strings.HasPrefix(segment, ".") {
			newPathSegments := append([]string{currentDirectory}, segments[i:]...)
			return path.Join(newPathSegments...), i != 0
		}

		if segment == ".." {
			currentDirectory = path.Dir(currentDirectory)
		}
	}

	return currentDirectory, true
}
