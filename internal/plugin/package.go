package plugin

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	"golang.org/x/tools/go/packages"
	"sync"
)

var pkg *Package
var oncePkg sync.Once

func getPackage() *Package {
	oncePkg.Do(func() {
		pkg = NewPackage()
	})
	return pkg
}

type Package struct {
	standardPkg map[string]*packages.Package
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
	err := p.scanPackage(ctx, location, func(ctx context.Context, pkgs []*packages.Package) (bool, error) {
		if len(pkgs[0].Errors) == 0 {
			p.nonStandardPackages(pkgs[0], &results)
			return false, nil
		}
		return true, nil
	})
	return results, err
}

func (p *Package) scanPackage(ctx context.Context, location string, visitor func(ctx context.Context, pkgs []*packages.Package) (bool, error)) error {
	objects, _ := afs.New().List(ctx, location)
	if len(objects) == 0 {
		return nil
	}
	for _, candidate := range objects {
		if !candidate.IsDir() {
			continue
		}
		dir := url.Path(candidate.URL())
		localPackages, _ := packages.Load(&packages.Config{Mode: packages.NeedModule | packages.NeedImports, Dir: dir}, "")
		if len(localPackages) == 0 {
			continue
		}
		if toContinue, err := visitor(ctx, localPackages); !toContinue {
			return err
		}
		if url.Equals(location, candidate.URL()) {
			continue
		}
		p.scanPackage(ctx, candidate.URL(), visitor)
	}
	return nil
}

func (p *Package) nonStandardPackages(src *packages.Package, result *Packages) {
	if len(src.Imports) == 0 {
		return
	}
	for k, pkg := range src.Imports {
		if _, ok := p.standardPkg[k]; !ok {
			result.Append(pkg)
		}
		if len(pkg.Imports) > 0 {
			if src.Module == nil {
				p.nonStandardPackages(pkg, result)
			}
			if pkg.Module != nil && src.Module != nil && pkg.Module.Path == src.Module.Path {
				p.nonStandardPackages(pkg, result)
			}
		}
	}
}

//NewPackage returns package informer
func NewPackage() *Package {
	stdPackage, _ := packages.Load(nil, "std")
	return &Package{standardPkg: Packages(stdPackage).Index()}
}
