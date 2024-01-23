package plugin

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	"go/ast"
	"go/parser"
	"go/token"
	"golang.org/x/mod/modfile"
	"golang.org/x/tools/go/packages"
	"path"
	"runtime"
	"strings"
)

func getPackage(modLocation string, modFile *modfile.File) *Package {
	return NewPackage(modLocation, modFile)
}

type Package struct {
	fs          afs.Service
	standardPkg map[string]*packages.Package
	modRoot     string
	modFile     *modfile.File
}

func (p *Package) Package(location string) (*packages.Package, error) {
	pkgs, err := packages.Load(&packages.Config{Mode: packages.NeedImports, Dir: location})
	if err != nil {
		return nil, err
	}
	return pkgs[0], nil
}

func (p *Package) NonStandard(ctx context.Context, location string) (Packages, error) {
	var results Packages
	err := p.scanPackage(ctx, location, func(ctx context.Context, pkg *packages.Package) (bool, error) {
		if pkg != nil && pkg.ID != "" {
			p.nonStandardPackages(pkg, &results)
			return true, nil
		}
		return true, nil
	})
	return results, err
}

func (p *Package) scanPackage(ctx context.Context, location string, visitor func(ctx context.Context, pkg *packages.Package) (bool, error)) error {
	fs := afs.New()
	p.ensureModule(ctx, location, fs)

	objects, _ := fs.List(ctx, location)
	if len(objects) == 0 {
		return nil
	}
	for _, candidate := range objects {
		if !candidate.IsDir() {
			continue
		}
		aPkg, _ := p.LoadImports(ctx, candidate.URL())
		if aPkg == nil || aPkg.ID == "" {
			continue
		}
		if toContinue, err := visitor(ctx, aPkg); !toContinue {
			return err
		}
		if url.Equals(location, candidate.URL()) {
			continue
		}
		if err := p.scanPackage(ctx, candidate.URL(), visitor); err != nil {
			return err
		}
	}
	return nil
}

func (p *Package) ensureModule(ctx context.Context, location string, fs afs.Service) {
	if p.modFile == nil {

		if ok, _ := fs.Exists(ctx, path.Join(location, "go.mod")); ok {
			if data, _ := fs.DownloadWithURL(ctx, path.Join(location, "go.mod")); len(data) > 0 {
				p.modFile, _ = modfile.Parse("", data, nil)
			}
		}
	}
}

func (p *Package) LoadImports(ctx context.Context, location string) (*packages.Package, error) {

	ret := &packages.Package{Imports: map[string]*packages.Package{}}
	objects, _ := afs.New().List(ctx, location)
	if len(objects) == 0 {
		return ret, nil
	}

	relative := ""
	if p.modRoot != "" {
		locationPath := url.Path(location)
		index := strings.Index(locationPath, p.modRoot)
		if index != -1 {
			relative = locationPath[index+len(p.modRoot):]
		}
		if p.modFile != nil {
			ret.ID = path.Join(p.modFile.Module.Mod.Path, relative)
		}
	}

	for _, candidate := range objects {
		if candidate.IsDir() {
			continue
		}
		if path.Ext(candidate.Name()) != ".go" {
			continue
		}
		data, err := p.fs.Download(ctx, candidate)
		if err != nil {
			return nil, err
		}
		fset := token.NewFileSet() // positions are relative to fset
		f, err := parser.ParseFile(fset, "src.go", data, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to parse: %v, %v", candidate.URL(), err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch actual := n.(type) {
			case *ast.File:
				if p.modFile != nil && p.modFile.Module != nil {
					parent, _ := path.Split(relative)
					if parent != "" {
						ret.ID = path.Join(p.modFile.Module.Mod.Path, parent, actual.Name.Name)
					} else {
						ret.ID = path.Join(p.modFile.Module.Mod.Path, actual.Name.Name)
					}
				} else {
					ret.ID = actual.Name.Name
				}
				for _, imps := range actual.Imports {
					pkgId := strings.Trim(imps.Path.Value, `"`)
					ret.Imports[pkgId] = &packages.Package{ID: pkgId}
				}
				return false
			}
			return true
		})

	}
	return ret, nil
}

func (p *Package) nonStandardPackages(src *packages.Package, result *Packages) {
	if len(src.Imports) == 0 {
		return
	}
	for k, pkgImps := range src.Imports {
		if p.modFile != nil && p.modFile.Module != nil && strings.HasPrefix(k, p.modFile.Module.Mod.Path) {
			continue
		}
		if _, ok := p.standardPkg[k]; !ok {
			result.Append(pkgImps)
		}
		if len(pkgImps.Imports) > 0 {
			if src.Module == nil {
				p.nonStandardPackages(pkgImps, result)
			}
			if pkgImps.Module != nil && src.Module != nil && pkgImps.Module.Path == src.Module.Path {
				p.nonStandardPackages(pkgImps, result)
			}
		}
	}
}

var standardPackages Packages

func getStandardPackages() Packages {
	if len(standardPackages) > 0 {
		return standardPackages
	}
	fs := afs.New()
	var result Packages
	parentURL := path.Join(runtime.GOROOT(), "src")
	discoverPackage(fs, parentURL, "", func(URL string, id string) {
		result = append(result, &packages.Package{ID: id, Name: id})
	})
	standardPackages = result
	return result
}

func discoverPackage(fs afs.Service, parentURL, prefix string, fn func(URL string, id string)) {
	objects, _ := fs.List(context.Background(), parentURL)
	for _, object := range objects {
		if url.Equals(object.URL(), parentURL) {
			continue
		}
		if object.IsDir() {
			id := object.Name()
			if prefix != "" {
				id = prefix + "/" + id
			}
			fn(object.URL(), id)
			discoverPackage(fs, object.URL(), id, fn)
		}
	}
}

// NewPackage returns package informer
func NewPackage(modFileLocation string, modFile *modfile.File) *Package {
	if strings.HasSuffix(modFileLocation, "go.mod") {
		modFileLocation = modFileLocation[:len(modFileLocation)-len("go.mod")]
	}
	return &Package{modFile: modFile, standardPkg: getStandardPackages().Index(), fs: afs.New(), modRoot: url.Path(modFileLocation)}
}
