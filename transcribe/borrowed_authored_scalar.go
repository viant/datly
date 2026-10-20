package transcribe

import (
	"fmt"
	"go/ast"
	"go/types"
	"path/filepath"
	"strings"

	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/internal/dependency"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
)

// Reuse the build-selected AST and its original bytes. Generated declarations
// and authored structs cannot supply the fresh SQL row or its named row graph.
func borrowedAuthoredScalarNames(snapshot *dependency.Snapshot, owner, directory string) map[string]bool {
	result := map[string]bool{}
	if snapshot == nil || snapshot.Packages[owner] == nil {
		return result
	}
	localTypes := map[string]bool{}
	for _, typ := range snapshot.Packages[owner].Types {
		if typ != nil {
			localTypes[typ.Name] = true
		}
	}
	for _, file := range snapshot.Packages[owner].Files {
		authored := false
		for _, sealed := range snapshot.Files {
			if filepath.Clean(sealed.Path) == filepath.Join(directory, filepath.Base(file.Name)) && !astGeneratedSource(string(sealed.Bytes)) {
				for _, typ := range file.Types {
					if typ.PkgPath == owner {
						authored = true
						break
					}
				}
				if authored {
					break
				}
			}
		}
		if !authored {
			continue
		}
		for _, typ := range file.Types {
			if typ == nil || typ.TypeSpec == nil || typ.TypeSpec.Assign.IsValid() {
				continue
			}
			ident, ok := typ.TypeSpec.Type.(*ast.Ident)
			if !ok {
				continue
			}
			if localTypes[ident.Name] {
				continue
			} // A package declaration can shadow a builtin.
			builtin := types.Universe.Lookup(ident.Name)
			if builtin == nil {
				continue
			}
			basic, ok := builtin.Type().Underlying().(*types.Basic)
			if ok && basic.Info()&(types.IsBoolean|types.IsInteger|types.IsFloat|types.IsComplex|types.IsString) != 0 {
				result[typ.Name] = true
			}
		}
	}
	return result
}

func borrowedScalarDependencies(contract gen.BorrowedLeafContract, catalog *typecatalog.Catalog) (map[string]bool, error) {
	result := map[string]bool{}
	if catalog == nil {
		return result, nil
	}
	for _, field := range contract.Fields {
		_, err := (xshape.Resolver{Rewriter: func(key string) (string, error) {
			if strings.HasPrefix(key, contract.Package+".") {
				origin, ok, err := catalog.Origin(typecatalog.TranscribeAuthority, key)
				if err != nil {
					return "", err
				}
				if ok && origin == typecatalog.TypeOriginPackage {
					result[strings.TrimPrefix(key, contract.Package+".")] = true
				}
			}
			return key, nil
		}}).Rewrite(field.Type)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// The existing owner-sealing pass already walks these AST declarations; do not
// reparse Go or SQL merely to locate an authored scalar's declaration/methods.
func borrowedScalarSource(file *ast.File, names map[string]bool) bool {
	for _, decl := range file.Decls {
		switch value := decl.(type) {
		case *ast.GenDecl:
			for _, spec := range value.Specs {
				if typ, ok := spec.(*ast.TypeSpec); ok && names[typ.Name.Name] {
					return true
				}
			}
		case *ast.FuncDecl:
			if value.Recv == nil || len(value.Recv.List) != 1 {
				continue
			}
			receiver := value.Recv.List[0].Type
			if pointer, ok := receiver.(*ast.StarExpr); ok {
				receiver = pointer.X
			}
			if ident, ok := receiver.(*ast.Ident); ok && names[ident.Name] {
				return true
			}
		}
	}
	return false
}

func retainBorrowedScalarSource(proof *gen.BorrowedRowAuthority, snapshot *dependency.Snapshot, current gen.BorrowedAuthorityFile) error {
	if snapshot != nil {
		for _, captured := range snapshot.Files {
			if captured.Path == current.Path {
				if err := gen.RetainBorrowedSeal(proof, captured); err != nil {
					return err
				}
				return gen.RetainBorrowedSeal(proof, current)
			}
		}
	}
	return fmt.Errorf("borrow_sql_row authored scalar source was not captured: %s", current.Path)
}

// Test and build-excluded sources never supplied descriptor authority.
func capturedBorrowedScalarFile(snapshot *dependency.Snapshot, path string) bool {
	if snapshot == nil {
		return false
	}
	for _, file := range snapshot.Files {
		if file.Path == path {
			return true
		}
	}
	return false
}
