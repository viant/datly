package generate

import (
	"github.com/viant/x"
	smodel "github.com/viant/x/syntetic/model"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
)

// sourceDescriptors projects canonical emitted declarations without a package
// load, filesystem mutation or recursive generation plan.
func sourceDescriptors(packagePath, source string) ([]*x.Type, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "prospective_contracts.go", source, 0)
	if err != nil {
		return nil, err
	}
	imports := map[string]*smodel.ImportRef{}
	for _, imp := range file.Imports {
		location, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			return nil, err
		}
		alias := filepath.Base(location)
		if imp.Name != nil {
			alias = imp.Name.Name
		}
		imports[alias] = &smodel.ImportRef{Path: location, Alias: alias}
	}
	var result []*x.Type
	for _, decl := range file.Decls {
		group, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, entry := range group.Specs {
			typ, ok := entry.(*ast.TypeSpec)
			if !ok {
				continue
			}
			result = append(result, &x.Type{Name: typ.Name.Name, PkgPath: packagePath, SynteticType: &smodel.Type{Name: typ.Name.Name, PkgPath: packagePath, TypeSpec: typ, Imports: imports}})
		}
	}
	return result, nil
}
