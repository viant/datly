package options

import (
	"context"
	"fmt"
	"github.com/viant/afs/url"
	"github.com/viant/toolbox/data"
	"golang.org/x/mod/modfile"
	"os"
	"path"
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
		GitFolder     *string
		GitPrivate    *string `short:"T" long:"gitprivate" description:"git private"`
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

func (e *Extension) Init(ctx context.Context) error {
	if e.Project == "" {
		e.Project, _ = os.Getwd()
	}
	e.Project = ensureAbsPath(e.Project)

	goMod, loc, err := tryLoadModule(ctx, e.PackageLocation())
	if err != nil {
		return fmt.Errorf("invalid %v %w", path.Join(e.PackageLocation(), "go.mod"), err)
	}

	if goMod != nil {
		index := strings.LastIndex(goMod.Module.Mod.Path, "/")
		name := goMod.Module.Mod.Path
		gitRepository := ""
		if e.GitRepository == nil {
			e.GitRepository = &gitRepository
		}
		if index != -1 {
			gitRepository = goMod.Module.Mod.Path[:index]
			name = goMod.Module.Mod.Path[index+1:]
		}
		e.GitFolder = &loc
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

func tryLoadModule(ctx context.Context, loc string) (*modfile.File, string, error) {
	pkgMod := url.Join(loc, "go.mod")
	if ok, _ := fs.Exists(ctx, pkgMod); ok {
		ret, err := loadModFile(ctx, pkgMod)
		return ret, "", err
	}
	parent, leaf := path.Split(loc)
	pkgMod = url.Join(parent, "go.mod")
	if ok, _ := fs.Exists(ctx, pkgMod); ok {
		ret, err := loadModFile(ctx, pkgMod)
		if err != nil {
			return nil, "", err
		}
		return ret, leaf, nil
	}
	return nil, "", nil
}

func loadModFile(ctx context.Context, pkgMod string) (*modfile.File, error) {
	data, err := fs.DownloadWithURL(ctx, pkgMod)
	if err != nil {
		return nil, err
	}
	return modfile.Parse(pkgMod, data, nil)
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
	impModule := module
	if e.GitFolder != nil {
		impModule = path.Join(module, *e.GitFolder)
	}
	replacer.Put("impModule", impModule)

	if index := strings.Index(name, "-"); index != -1 {
		name = name[index+1:]
	}
	replacer.Put("moduleName", name)
	modulePath := e.Project
	if e.GitFolder == nil || *e.GitFolder == "" {
		modulePath = path.Join(modulePath, "pkg")
	}
	replacer.Put("modulePath", modulePath)
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
