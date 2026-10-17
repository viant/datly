package transcribe

import (
	"fmt"
	"go/ast"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
	smodel "github.com/viant/x/syntetic/model"
)

func factorySourceLookup(ownership *factoryDeclarations, resolver *typecatalog.Resolver) (xshape.Lookup, error) {
	if err := ownership.load(); err != nil {
		return nil, err
	}
	return func(key string) (*x.Type, error) {
		if strings.HasPrefix(key, ownership.pkg+".") {
			name := strings.TrimPrefix(key, ownership.pkg+".")
			if declarations := ownership.types[name]; len(declarations) != 0 {
				if len(declarations) != 1 {
					return nil, fmt.Errorf("factory source declaration %s is ambiguous", key)
				}
				declaration := declarations[0]
				imports := map[string]*smodel.ImportRef{}
				for _, imp := range declaration.imports {
					pkg, err := strconv.Unquote(imp.Path.Value)
					if err != nil {
						return nil, err
					}
					alias := path.Base(pkg)
					if imp.Name != nil {
						alias = imp.Name.Name
					}
					imports[alias] = &smodel.ImportRef{Alias: alias, Path: pkg}
				}
				return &x.Type{Name: name, PkgPath: ownership.pkg, SynteticType: &smodel.Type{Name: name, PkgPath: ownership.pkg, TypeSpec: declaration.typeSpec, Imports: imports}}, nil
			}
		}
		return resolver.Descriptor(key)
	}, nil
}

// factoryCanonicalIdentity expands real Go aliases using their existing source
// AST. Defined types retain nominal identity, including generic instantiations.
func factoryCanonicalIdentity(expression string, lookup xshape.Lookup) (string, error) {
	visiting := map[string]bool{}
	var rewrite func(string) (string, error)
	rewrite = func(name string) (string, error) {
		if !strings.Contains(name, ".") {
			return name, nil
		}
		descriptor, err := lookup(name)
		if err != nil {
			return "", err
		}
		if descriptor == nil || descriptor.SynteticType == nil || descriptor.SynteticType.TypeSpec == nil || !descriptor.SynteticType.TypeSpec.Assign.IsValid() {
			return name, nil
		}
		if visiting[name] {
			return "", fmt.Errorf("factory source alias cycle at %s", name)
		}
		visiting[name] = true
		defer delete(visiting, name)
		source := descriptor.SynteticType
		if source.TypeSpec.TypeParams != nil && len(source.TypeSpec.TypeParams.List) != 0 {
			return "", fmt.Errorf("factory generic alias %s requires existing instantiated source authority", name)
		}
		imports := map[string]string{}
		for alias, imp := range source.Imports {
			if imp != nil {
				imports[alias] = imp.Path
			}
		}
		target, err := (xshape.Resolver{Package: descriptor.PkgPath, Imports: imports}).Canonical(source.TypeSpec.Type)
		if err != nil {
			return "", err
		}
		return (xshape.Resolver{Rewriter: rewrite}).Rewrite(target)
	}
	return (xshape.Resolver{Rewriter: rewrite}).Rewrite(expression)
}

// loadOwnerSnapshot adapts the dependency owner's already parsed declarations;
// the complete owner Go source is never parsed again for factory admission.
func (d *factoryDeclarations) loadOwnerSnapshot() error {
	d.types = map[string][]factoryDeclaration{}
	d.functions = map[string]bool{}
	d.files = map[string]string{}
	for _, sealed := range d.ownerFiles {
		if filepath.Clean(filepath.Dir(sealed.Path)) == filepath.Clean(d.directory) && strings.HasSuffix(sealed.Path, ".go") {
			d.files[filepath.Base(sealed.Path)] = string(sealed.Bytes)
		}
	}
	for _, file := range d.ownerPackage.Files {
		name := filepath.Base(file.Name)
		source, ok := d.files[name]
		if !ok {
			return fmt.Errorf("owner source file %s has no consumed-byte snapshot", name)
		}
		imports := []*ast.ImportSpec{}
		for _, imp := range file.Imports {
			name := (*ast.Ident)(nil)
			if imp.Alias != "" {
				name = ast.NewIdent(imp.Alias)
			}
			imports = append(imports, &ast.ImportSpec{Name: name, Path: &ast.BasicLit{Value: strconv.Quote(imp.Path)}})
		}
		for _, typ := range file.Types {
			if typ.TypeSpec == nil {
				return fmt.Errorf("owner source type %s has no original AST", typ.Name)
			}
			_, structure := typ.TypeSpec.Type.(*ast.StructType)
			d.types[typ.Name] = append(d.types[typ.Name], factoryDeclaration{file: name, source: source, generated: astGeneratedSource(source), concreteStruct: structure && !typ.TypeSpec.Assign.IsValid(), typeSpec: typ.TypeSpec, imports: imports})
		}
	}
	for _, fn := range d.ownerPackage.Funcs {
		if fn.Decl != nil && fn.Decl.Recv == nil {
			d.functions[fn.Name] = true
		}
	}
	return nil
}

func astGeneratedSource(source string) bool {
	for _, line := range strings.Split(source, "\n") {
		if strings.HasPrefix(line, "package ") {
			return false
		}
		if strings.HasPrefix(line, "// Code generated ") && strings.HasSuffix(line, " DO NOT EDIT.") {
			return true
		}
	}
	return false
}
