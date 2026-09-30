package packageasset

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
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
	sourceFiles := map[string]string{}
	for _, filename := range files {
		if strings.HasSuffix(filename, "_test.go") {
			continue
		}
		content, err := os.ReadFile(filename)
		if err != nil {
			return nil, err
		}
		if !strings.Contains(string(content), resourceNamespaceSuffix) && !strings.Contains(string(content), resourceVariableSuffix) {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), filename, content, parser.ParseComments)
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
					sourceFiles[strings.TrimSuffix(value.Names[0].Name, resourceVariableSuffix)] = filepath.Base(filename)
				}
			}
		}
	}
	result := make([]*Resources, 0, len(resources))
	seen := map[string]bool{}
	for symbol, patterns := range resources {
		namespace := namespaces[symbol]
		if namespace == "" {
			return nil, fmt.Errorf("embedded resource %s has no matching namespace constant", symbol)
		}
		item := &Resources{Namespace: namespace, Files: patterns, SourceFile: sourceFiles[symbol], Symbol: symbol}
		if err := item.Validate(); err != nil {
			return nil, err
		}
		if seen[namespace] {
			return nil, fmt.Errorf("duplicate resource namespace %s", namespace)
		}
		seen[namespace] = true
		item.Files, err = ExpandSourceResources(directory, patterns)
		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Namespace < result[j].Namespace })
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
				if !fs.ValidPath(strings.TrimPrefix(value, "all:")) || value == "." {
					return nil, fmt.Errorf("invalid go:embed resource path %q", value)
				}
				result = append(result, value)
			}
		}
	}
	return result, nil
}

// SourceEmbedFiles returns the assets named by a Go file's embed directives.
// It is also used to retire templates referenced by obsolete generated code.
func SourceEmbedFiles(directory string, content []byte) ([]string, error) {
	parsed, err := parser.ParseFile(token.NewFileSet(), "resources.go", content, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	var patterns []string
	for _, group := range parsed.Comments {
		found, err := sourceEmbedPatterns(group)
		if err != nil {
			return nil, err
		}
		patterns = append(patterns, found...)
	}
	return ExpandSourceResources(directory, patterns)
}

// ExpandSourceResources follows Go embed file/directory/glob selection. Missing
// literal files remain explicit so snapshot loading reports the broken contract.
func ExpandSourceResources(directory string, patterns []string) ([]string, error) {
	source := os.DirFS(directory)
	seen := map[string]bool{}
	for _, pattern := range patterns {
		all := strings.HasPrefix(pattern, "all:")
		pattern = strings.TrimPrefix(pattern, "all:")
		matches, err := fs.Glob(source, pattern)
		if err != nil {
			return nil, err
		}
		if len(matches) == 0 {
			if strings.ContainsAny(pattern, "*?[") {
				return nil, fmt.Errorf("go:embed pattern %q has no matching resources", pattern)
			}
			matches = []string{pattern}
		}
		for _, match := range matches {
			info, err := fs.Stat(source, match)
			if os.IsNotExist(err) {
				seen[match] = true
				continue
			}
			if err != nil {
				return nil, err
			}
			if !info.IsDir() {
				seen[match] = true
				continue
			}
			err = fs.WalkDir(source, match, func(name string, entry fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if !all && (strings.HasPrefix(entry.Name(), ".") || strings.HasPrefix(entry.Name(), "_")) {
					if entry.IsDir() {
						return fs.SkipDir
					}
					return nil
				}
				if !entry.IsDir() {
					seen[name] = true
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
		}
	}
	result := make([]string, 0, len(seen))
	for name := range seen {
		result = append(result, name)
	}
	sort.Strings(result)
	return result, nil
}
