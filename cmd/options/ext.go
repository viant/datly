package options

import (
	"context"
	"fmt"
	"github.com/viant/afs/url"
	"github.com/viant/toolbox/data"
	"golang.org/x/mod/modfile"
	"os"
	"strings"
	"time"
)

type (
	Extension struct {
		Project string `short:"p" long:"proj" description:"destination project"`
		Module
		Datly
	}
	Module struct {
		GitRepository *string `short:"g" long:"gitrepo" description:"git module repo"`
		Name          string  `short:"n" long:"name" description:"module name" `
	}
	Datly struct {
		Location string `short:"x" long:"dsrc" description:"datly location" default:".build"`
		Tag      string `short:"t" long:"tag" description:" datly tag"`
	}
)

const pkgFolder = "pkg"

func (e *Extension) PackageLocation() string {
	pkgDest := url.Join(e.Project, pkgFolder)
	return pkgDest
}

func (e *Extension) Init() error {
	if e.Project == "" {
		e.Project, _ = os.Getwd()
	}
	e.Project = ensureAbsPath(e.Project)

	pkgMod := url.Join(e.PackageLocation(), "go.mod")
	if ok, _ := fs.Exists(context.Background(), pkgMod); ok {
		data, _ := fs.DownloadWithURL(context.Background(), pkgMod)
		goMod, err := modfile.Parse(pkgMod, data, nil)
		if err != nil {
			return fmt.Errorf("invalid %v %w", pkgMod, err)
		}
		index := strings.LastIndex(goMod.Module.Mod.Path, "/")
		gitRepository := goMod.Module.Mod.Path[:index]
		name := goMod.Module.Mod.Path[index+1:]
		if e.GitRepository == nil {
			e.GitRepository = &gitRepository
		}
		if e.Name == "" {
			e.Name = name
		}
		if *e.GitRepository != gitRepository {
			return fmt.Errorf("invalid repository:  %v, but expected %v", *e.GitRepository, gitRepository)
		}
		if e.Name != name {
			return fmt.Errorf("invalid git module name:  %v, but expected %v", e.Name, name)
		}
	}
	if e.Datly.Location == "" {
		e.Datly.Location = ".build"
	}

	if url.IsRelative(e.Datly.Location) {
		e.Datly.Location = url.Join(e.Project, e.Datly.Location)
	}

	if e.GitRepository == nil {
		repo := "github.com/" + os.Getenv("USER")
		e.GitRepository = &repo
	}
	if e.Name == "" {
		e.Name = "myapp"
	}
	return nil
}

func (e *Module) Module() string {
	if e.GitRepository == nil {
		return e.Name
	}
	return *e.GitRepository + "/" + e.Name
}

func (e *Extension) Replacer(shared *Module) data.Map {
	var replacer = data.Map{}
	now := time.Now().UTC().Format(time.RFC3339)
	module := shared.Module()
	name := extractModuleName(module)

	replacer.Put("module", module)
	if index := strings.Index(name, "-"); index != -1 {
		name = name[index+1:]
	}
	replacer.Put("moduleName", name)
	replacer.Put("modulePath", url.Join(e.Project, "pkg"))
	replacer.Put("extModulePath", url.Join(e.Project, ".build/ext"))
	replacer.Put("generatedAt", now)
	return replacer
}

func extractModuleName(name string) string {
	if index := strings.LastIndex(name, "."); index != -1 {
		name = name[index+1:]
	}
	if index := strings.LastIndex(name, "/"); index != -1 {
		name = name[index+1:]
	}
	return name
}

func (e *Extension) GoModInitArgs(shared *Module) []string {
	return []string{
		"mod",
		"init",
		shared.Module(),
	}
}
