package dependency

import (
	"go/ast"
	"path"
	"sort"

	smodel "github.com/viant/x/syntetic/model"
)

// declarationImports excludes dependencies used only by executable code.
// Import scopes belong to declarations and methods, not to the whole package.
func declarationImports(pkg *smodel.Package) []smodel.ImportRef {
	required := map[string]smodel.ImportRef{}
	constants := map[string]smodel.ConstDecl{}
	visited := map[string]bool{}
	// The loader omits dot imports from individual declaration alias maps.
	for _, imported := range pkg.Imports {
		if imported.Alias == "." {
			required[imported.Path] = imported
		}
	}
	for _, constant := range pkg.Consts {
		constants[constant.Name] = constant
	}
	var scan func(ast.Node, map[string]*smodel.ImportRef)
	scan = func(node ast.Node, imports map[string]*smodel.ImportRef) {
		if node == nil {
			return
		}
		for alias, imp := range imports {
			if imp != nil && (alias == "." || imp.Alias == ".") {
				required[imp.Path] = *imp
			}
		}
		ast.Inspect(node, func(n ast.Node) bool {
			switch item := n.(type) {
			case *ast.TypeSpec:
				if item.TypeParams != nil {
					scan(item.TypeParams, imports)
				}
				scan(item.Type, imports)
				return false
			case *ast.Field:
				scan(item.Type, imports)
				return false
			case *ast.SelectorExpr:
				if qualifier, ok := item.X.(*ast.Ident); ok {
					if imp := imports[qualifier.Name]; imp != nil {
						required[imp.Path] = *imp
					}
				}
				return false
			case *ast.Ident:
				if constant, ok := constants[item.Name]; ok && !visited[item.Name] {
					visited[item.Name] = true
					if constant.Value == nil {
						// The source model omits inherited const expressions.
						// Keep the declaring file's imports rather than lose a
						// dependency used by an array length through that constant.
						for _, file := range pkg.Files {
							for _, declaration := range file.Consts {
								if declaration.Name == constant.Name {
									for _, imp := range file.Imports {
										required[imp.Path] = imp
									}
									for _, candidate := range file.Consts {
										if candidate.Value != nil {
											scan(candidate.Value, nil)
										}
										if candidate.Type != nil {
											scan(candidate.Type, nil)
										}
									}
								}
							}
						}
					}
					scope := map[string]*smodel.ImportRef{}
					for _, imp := range constant.Imports {
						required[imp.Path] = imp
						alias := imp.Alias
						if alias == "" {
							alias = path.Base(imp.Path)
						}
						copy := imp
						scope[alias] = &copy
					}
					if constant.Type != nil {
						scan(constant.Type, scope)
					}
					if constant.Value != nil {
						scan(constant.Value, scope)
					}
				}
			}
			return true
		})
	}
	for _, typ := range pkg.Types {
		if typ == nil {
			continue
		}
		if typ.TypeSpec != nil {
			scan(typ.TypeSpec, typ.Imports)
		}
		for _, methods := range [][]*ast.FuncDecl{typ.MethodsAST, typ.PtrMethodsAST} {
			for _, method := range methods {
				if method != nil && method.Type != nil {
					scan(method.Type, typ.ImportsForMethod(method.Name.Name))
				}
			}
		}
	}
	result := make([]smodel.ImportRef, 0, len(required))
	for _, imp := range required {
		result = append(result, imp)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Path < result[j].Path })
	return result
}
