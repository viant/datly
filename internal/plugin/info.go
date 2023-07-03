package plugin

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	"github.com/viant/datly/config"
	"github.com/viant/xreflect"
	"golang.org/x/mod/modfile"
	"golang.org/x/tools/go/packages"
	"path"
)

const (
	customModule        = "github.com/viant/xdatly/types/custom"
	typesCorePkg        = "github.com/viant/xdatly/types/core"
	dependencyDirectory = "dependency"
	checksumDirectory   = "checksum"
	pluginDirectory     = "plugin"
)

type Mode string //Extension|CustomModule

const (
	ModeUndefined = Mode("")

	ModeStandalone       = Mode("standalone")
	ModeExtension        = Mode("extension")
	ModeCustomTypeModule = Mode("custom")
)

type (
	Info struct {
		fs afs.Service

		URL                 string
		Mod                 *modfile.File
		NonStandardPackages Packages
		CustomTypesPackages Packages
		IntegrationMode     Mode
		HasMethod           bool
	}
)

func (i *Info) Package(pkg string) string {
	if i.Mod == nil {
		return "main"
	}
	if pkg == "" {
		pkg = "autogen"
	}
	return pkg
}

func (i *Info) IsStandalone() bool {
	return i.IntegrationMode == ModeStandalone
}

func (i *Info) init(ctx context.Context) error {
	if err := i.tryLoadModFile(); err != nil {
		return err
	}
	if err := i.detectDependencies(ctx); err != nil {
		return err
	}
	if err := i.detectCustomTypes(ctx, i.URL); err != nil {
		return err
	}
	i.detectLocalMethods(ctx)
	return nil
}

func (i *Info) DependencyURL() string {
	return url.Join(i.URL, dependencyDirectory, "init.go")
}

func (i *Info) DependencyPkg() string {
	if i.Mod == nil || i.Mod.Module == nil {
		return ""
	}
	return path.Join(i.Mod.Module.Mod.Path, dependencyDirectory)
}
func (i *Info) ChecksumPkg() string {
	if i.Mod == nil || i.Mod.Module == nil {
		return ""
	}
	return path.Join(i.Mod.Module.Mod.Path, checksumDirectory)
}

func (i *Info) TypeCorePkg() string {
	return typesCorePkg
}

func (i *Info) ChecksumURL() string {
	return url.Join(i.URL, checksumDirectory, "init.go")
}

func (i *Info) PluginURL() string {
	return url.Join(i.URL, pluginDirectory, "main.go")
}

func (i *Info) tryLoadModFile() error {
	goModFile := url.Join(i.URL, "go.mod")
	fileContent, err := i.fs.DownloadWithURL(context.Background(), goModFile)
	if err != nil {
		return nil
	}
	i.Mod, err = modfile.Parse("go.mod", fileContent, nil)
	return err
}

func (i *Info) detectDependencies(ctx context.Context) error {
	if pkgs, _ := getPackage().NonStandard(ctx, i.URL); len(pkgs) > 0 {
		i.NonStandardPackages = pkgs
	}
	if i.Mod != nil {
		i.detectGoModDependencies()
		return nil
	}
	i.IntegrationMode = ModeStandalone
	if len(i.NonStandardPackages) > 0 {
		return fmt.Errorf("detected non go standard package in standalone mode: %v, please run go mod init", i.NonStandardPackages)
	}

	return nil
}

func (i *Info) detectGoModDependencies() {
	if module := i.Mod.Module; module != nil {
		if module.Mod.Path == customModule {
			i.IntegrationMode = ModeCustomTypeModule
		} else {
			i.IntegrationMode = ModeExtension
		}
	}
}

func (i *Info) detectCustomTypes(ctx context.Context, URL string) error {
	location := url.Path(URL)
	return getPackage().scanPackage(ctx, location, func(ctx context.Context, pkgs []*packages.Package) (bool, error) {
		if len(pkgs) == 0 {
			return true, nil
		}
		if len(pkgs[0].Errors) > 0 {
			return true, nil
		}
		if len(pkgs[0].Imports) > 0 {
			i.addTypesCorePackage(pkgs[0])
		}
		return true, nil
	})
}

func (i *Info) UpdateTypesCorePackage(URL string) {
	dir := url.Path(URL)
	pkgs, _ := packages.Load(&packages.Config{Mode: packages.NeedModule | packages.NeedImports, Dir: dir}, "")
	if len(pkgs) == 0 {
		return
	}
	i.addTypesCorePackage(pkgs[0])
}

func (i *Info) addTypesCorePackage(pkg *packages.Package) {
	if _, ok := pkg.Imports[typesCorePkg]; ok {
		i.CustomTypesPackages.Append(pkg)
	}
}

func (i *Info) detectLocalMethods(ctx context.Context) {
	if !i.IsStandalone() {
		return
	}
	dirTypes, err := xreflect.ParseTypes(i.URL, xreflect.WithTypeLookup(config.Config.Types.Lookup))
	if err != nil {
		return
	}

	for _, typeName := range dirTypes.TypesNames() {
		if methods := dirTypes.Methods(typeName); len(methods) > 0 {
			i.HasMethod = true
			return
		}
	}
}

func NewInfo(ctx context.Context, URL string) (*Info, error) {
	var fs = afs.New()
	if err := ensureValidDirectory(ctx, fs, URL); err != nil {
		return nil, err
	}
	info := &Info{fs: fs, URL: URL, NonStandardPackages: []*packages.Package{}}
	err := info.init(ctx)
	return info, err
}

func ensureValidDirectory(ctx context.Context, fs afs.Service, URL string) error {
	object, err := fs.Object(ctx, URL)
	if err != nil {
		return err
	}
	if !object.IsDir() {
		return fmt.Errorf("invalid URL: %v, expected folder location", URL)
	}
	return nil
}
