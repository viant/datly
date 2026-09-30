package generate

import (
	"fmt"
	"go/ast"
	"go/scanner"
	"go/token"
	"strings"

	xshape "github.com/viant/x/shape"
)

// MutationSource keeps generated policy support concerns in separate,
// package-local files under the same ownership and persistence transaction.
type MutationSource struct {
	Role        string
	Destination string
	File        *ast.File
}

func (s MutationSource) clone() (MutationSource, error) {
	file, err := cloneGoFile(s.File)
	if err != nil {
		return MutationSource{}, fmt.Errorf("clone mutation support %q: %w", s.Destination, err)
	}
	return MutationSource{Role: s.Role, Destination: s.Destination, File: file}, nil
}

func (s MutationSource) resolve(plan *Plan, targetPackage string) (MutationSource, error) {
	if s.File == nil {
		return MutationSource{}, fmt.Errorf("mutation support %q AST is required", s.Destination)
	}
	if s.Role != "" {
		s.Destination = plan.Generation.File(s.Role, s.Destination)
	}
	destination, err := packageGoDestination(s.Destination, "mutation support")
	if err != nil {
		return MutationSource{}, fmt.Errorf("mutation support destination: %w", err)
	}
	if err = validateHandlerComments(s.File); err != nil {
		return MutationSource{}, err
	}
	imports, err := handlerImports(s.File, targetPackage)
	if err != nil {
		return MutationSource{}, err
	}
	if err = validateContractHandlerImports(imports); err != nil {
		return MutationSource{}, err
	}
	for _, path := range imports {
		if path == "github.com/viant/datly/sql" || strings.HasPrefix(path, "github.com/viant/datly/sql/") {
			return MutationSource{}, fmt.Errorf("mutation support cannot import Datly SQL execution package %q", path)
		}
	}
	if err = validateHandlerDeclarations(s.File, "", plan); err != nil {
		return MutationSource{}, err
	}
	s.Destination = destination
	return s.clone()
}

func (s MutationSource) source(packageName string) (string, error) {
	if s.File == nil {
		return "", fmt.Errorf("mutation source AST is required")
	}
	// Only the package clause changes, so format the shared AST once and
	// rename the clause in the rendered text instead of cloning (which costs a
	// print, a parse and a second print) and formatting again.
	source, err := (xshape.SourceParser{}).FormatFile(s.File)
	if err != nil {
		return "", fmt.Errorf("format mutation source: %w", err)
	}
	renamed, err := renamePackageClause(source, packageName)
	if err != nil {
		return "", fmt.Errorf("rename mutation source package: %w", err)
	}
	return string(renamed), nil
}

// renamePackageClause replaces the identifier of the package clause in
// formatted Go source using the Go scanner, so comments or strings that
// mention "package" are never touched.
func renamePackageClause(source []byte, packageName string) ([]byte, error) {
	fset := token.NewFileSet()
	file := fset.AddFile("source.go", -1, len(source))
	var scan scanner.Scanner
	scan.Init(file, source, nil, 0)
	for {
		pos, tok, _ := scan.Scan()
		if tok == token.EOF {
			return nil, fmt.Errorf("package clause not found")
		}
		if tok != token.PACKAGE {
			continue
		}
		namePos, nameTok, name := scan.Scan()
		if nameTok != token.IDENT {
			return nil, fmt.Errorf("package clause at %s has no identifier", fset.Position(pos))
		}
		start := file.Offset(namePos)
		result := make([]byte, 0, len(source)+len(packageName))
		result = append(result, source[:start]...)
		result = append(result, packageName...)
		result = append(result, source[start+len(name):]...)
		return result, nil
	}
}
