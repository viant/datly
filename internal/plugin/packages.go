package plugin

import "golang.org/x/tools/go/packages"

type Packages []*packages.Package

func (p *Packages) Append(pkg *packages.Package) {
	if len(*p) == 0 {
		*p = append(*p, pkg)
		return
	}
	for _, item := range *p {
		if item.ID == pkg.ID {
			return
		}
	}
	*p = append(*p, pkg)
}
func (p Packages) Index() map[string]*packages.Package {
	var result = make(map[string]*packages.Package, len(p))
	for i := range p {
		result[p[i].ID] = p[i]
	}
	return result
}
