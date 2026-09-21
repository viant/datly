package packageasset

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	resourceNamespaceSuffix = "DatlyResourceNamespace"
	resourceVariableSuffix  = "DatlyResources"
)

// DiscoverSourceResources finds generated go:embed declarations in a package.
func DiscoverSourceResources(directory string) ([]*Resources, error) {
	files, err := filepath.Glob(filepath.Join(directory, "*.go"))
	if err != nil {
		return nil, err
	}
	namespaces := map[string]string{}
	resources := map[string][]string{}
	for _, filename := range files {
		if strings.HasSuffix(filename, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), filename, nil, parser.ParseComments)
		if err != nil {
			return nil, fmt.Errorf("parse package resources %s: %w", filename, err)
		}
		for _, declaration := range parsed.Decls {
			generated, ok := declaration.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, raw := range generated.Specs {
				value, ok := raw.(*ast.ValueSpec)
				if !ok {
					continue
				}
				if generated.Tok == token.CONST && len(value.Names) == 1 && len(value.Values) == 1 {
					name := value.Names[0].Name
					literal, ok := value.Values[0].(*ast.BasicLit)
					if ok && literal.Kind == token.STRING && strings.HasSuffix(name, resourceNamespaceSuffix) {
						decoded, decodeErr := strconv.Unquote(literal.Value)
						if decodeErr != nil {
							return nil, decodeErr
						}
						namespaces[strings.TrimSuffix(name, resourceNamespaceSuffix)] = decoded
					}
				}
				if generated.Tok != token.VAR || len(value.Names) != 1 || !strings.HasSuffix(value.Names[0].Name, resourceVariableSuffix) {
					continue
				}
				patterns, parseErr := sourceEmbedPatterns(generated.Doc, value.Doc)
				if parseErr != nil {
					return nil, fmt.Errorf("parse package resources %s: %w", filename, parseErr)
				}
				if len(patterns) > 0 {
					resources[strings.TrimSuffix(value.Names[0].Name, resourceVariableSuffix)] = patterns
				}
			}
		}
	}
	result := make([]*Resources, 0, len(resources))
	for symbol, patterns := range resources {
		namespace := namespaces[symbol]
		if namespace == "" {
			return nil, fmt.Errorf("embedded resource %s has no matching namespace constant", symbol)
		}
		item := &Resources{Namespace: namespace, Files: patterns}
		if err := item.Validate(); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, nil
}

func sourceEmbedPatterns(groups ...*ast.CommentGroup) ([]string, error) {
	var result []string
	for _, group := range groups {
		if group == nil {
			continue
		}
		for _, comment := range group.List {
			text := strings.TrimSpace(strings.TrimPrefix(comment.Text, "//"))
			if !strings.HasPrefix(text, "go:embed") {
				continue
			}
			for _, value := range strings.Fields(strings.TrimSpace(strings.TrimPrefix(text, "go:embed"))) {
				if strings.HasPrefix(value, "\"") || strings.HasPrefix(value, "`") {
					decoded, err := strconv.Unquote(value)
					if err != nil {
						return nil, err
					}
					value = decoded
				}
				if !fs.ValidPath(value) || value == "." {
					return nil, fmt.Errorf("invalid go:embed resource path %q", value)
				}
				result = append(result, value)
			}
		}
	}
	return result, nil
}
