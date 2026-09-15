package generate

import (
	"fmt"
	"go/ast"
	"path/filepath"
	"strings"

	xshape "github.com/viant/x/shape"
)

// MutationSource keeps generated policy support concerns in separate,
// package-local files under the same ownership and persistence transaction.
type MutationSource struct {
	Destination string
	File        *ast.File
}

func (s MutationSource) clone() (MutationSource, error) {
	file, err := cloneGoFile(s.File)
	if err != nil {
		return MutationSource{}, fmt.Errorf("clone mutation support %q: %w", s.Destination, err)
	}
	return MutationSource{Destination: s.Destination, File: file}, nil
}

func (s MutationSource) resolve(plan *Plan, targetPackage string) (MutationSource, error) {
	if s.File == nil {
		return MutationSource{}, fmt.Errorf("mutation support %q AST is required", s.Destination)
	}
	destination, err := managedRelativePath(strings.TrimSpace(s.Destination))
	if err != nil {
		return MutationSource{}, fmt.Errorf("mutation support destination: %w", err)
	}
	if filepath.Base(destination) != destination || filepath.Ext(destination) != ".go" {
		return MutationSource{}, fmt.Errorf("mutation support destination %q must be a package-local .go file", s.Destination)
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
