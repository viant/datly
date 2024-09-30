package signature

import (
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/xreflect"
)

// Signature defines contract signature
type Signature struct {
	URI       string
	Method    string
	Anonymous bool
	Types     []*view.TypeDefinition
	Output    *state.Schema
	Input     *state.Type
	Filter    *state.Schema
	//TODO add input, body with types def if needed
}

func (s *Signature) GoImports() xreflect.GoImports {
	imps := xreflect.GoImports{}
	var unique = map[string]bool{}
	for _, typeDef := range s.Types {
		if typeDef.ModulePath == "" && typeDef.Package == "" {
			continue
		}
		pkgPath := typeDef.ModulePath
		if pkgPath == "" {
			pkgPath = typeDef.Package
		}
		if unique[pkgPath] {
			continue
		}
		unique[pkgPath] = true

		_, name := url.Split(pkgPath, file.Scheme)
		imps = append(imps, &xreflect.GoImport{Name: name, Module: pkgPath})
	}
	return imps
}
