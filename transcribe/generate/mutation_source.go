package generate

import (
	"fmt"
	"go/ast"
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
	file, err := cloneGoFile(s.File)
	if err != nil {
		return "", err
	}
	file.Name.Name = packageName
	source, err := (xshape.SourceParser{}).FormatFile(file)
	if err != nil {
		return "", fmt.Errorf("format mutation source: %w", err)
	}
	return string(source), nil
}
