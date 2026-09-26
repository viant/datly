package build

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	xmodule "github.com/viant/x/module"
)

// LinkRequest makes source discovery explicit. Ordinary transcribe and build
// operations never rewrite the project's application-owned link package.
type LinkRequest struct {
	Dir, Tags string
	Env       []string
	Packages  []string
}

type LinkResult struct {
	Added []string `json:"added"`
}

type linkCandidate struct {
	dir, packageName   string
	holders, reachable map[string]bool
	hasInit            bool
}

// SyncLinks adds only missing blank imports to internal/datlylink/link.go.
// Runtime component discovery and exposure selection remain unchanged.
func (Service) SyncLinks(ctx context.Context, request LinkRequest) (*LinkResult, error) {
	root, err := filepath.Abs(request.Dir)
	if err != nil {
		return nil, err
	}
	info, err := xmodule.LocateLocal(root)
	if err != nil {
		return nil, err
	}
	if info.Dir != root {
		return nil, fmt.Errorf("link directory must be the module root: %s", info.Dir)
	}
	linkPath := filepath.Join(root, "internal", "datlylink", "link.go")
	original, err := os.ReadFile(linkPath)
	if err != nil {
		return nil, fmt.Errorf("existing project link file is required: %w", err)
	}
	selection, err := (xmodule.BuildWorkspace{BaseDir: root, Patterns: request.Packages, Tags: request.Tags, Env: request.Env}).Resolve(ctx)
	if err != nil {
		return nil, err
	}
	existing, err := linkImports(filepath.Dir(linkPath))
	if err != nil {
		return nil, err
	}
	wanted := map[string]*linkCandidate{}
	err = selection.Workspace().Walk(ctx, []string{"..."}, []string{info.Path + "/cmd/datly", info.Path + "/internal/datlylink"}, func(file xmodule.File) error {
		if !strings.HasSuffix(file.Path, ".go") || strings.HasSuffix(file.Path, "_test.go") ||
			!strings.HasPrefix(file.ImportPath, info.Path+"/") {
			return nil
		}
		relative, relErr := filepath.Rel(root, file.Dir)
		if relErr != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return fmt.Errorf("linked package is outside the project: %s", file.ImportPath)
		}
		candidate := wanted[file.ImportPath]
		if candidate == nil {
			candidate = &linkCandidate{dir: file.Dir, holders: map[string]bool{}, reachable: map[string]bool{}}
			wanted[file.ImportPath] = candidate
		}
		return candidate.inspect(file.Path)
	})
	if err != nil {
		return nil, err
	}
	added := make([]string, 0, len(wanted))
	for path, candidate := range wanted {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(candidate.holders) == 0 {
			continue
		}
		if err := candidate.ensureSupport(); err != nil {
			return nil, fmt.Errorf("prepare linked package %s: %w", path, err)
		}
		if !existing[path] {
			added = append(added, path)
		}
	}
	sort.Strings(added)
	if len(added) == 0 {
		return &LinkResult{Added: []string{}}, nil
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, linkPath, original, parser.PackageClauseOnly|parser.ImportsOnly)
	if err != nil {
		return nil, err
	}
	if file.Name == nil || file.Name.Name != "datlylink" {
		return nil, fmt.Errorf("link file must declare package datlylink")
	}
	offset := fset.File(file.Name.End()).Offset(file.Name.End())
	var imports strings.Builder
	imports.WriteString("\n\nimport (\n")
	for _, path := range added {
		fmt.Fprintf(&imports, "\t_ %q\n", path)
	}
	imports.WriteString(")")
	updated := make([]byte, 0, len(original)+imports.Len())
	updated = append(updated, original[:offset]...)
	updated = append(updated, imports.String()...)
	updated = append(updated, original[offset:]...)
	if _, err := parser.ParseFile(token.NewFileSet(), linkPath, updated, 0); err != nil {
		return nil, fmt.Errorf("updated link file is invalid: %w", err)
	}
	stat, err := os.Stat(linkPath)
	if err != nil {
		return nil, err
	}
	temp, err := os.CreateTemp(filepath.Dir(linkPath), ".datly-link-*.go")
	if err != nil {
		return nil, err
	}
	defer os.Remove(temp.Name())
	if err = temp.Chmod(stat.Mode().Perm()); err != nil {
		temp.Close()
		return nil, err
	}
	if _, err = temp.Write(updated); err != nil {
		temp.Close()
		return nil, err
	}
	if err = temp.Sync(); err != nil {
		temp.Close()
		return nil, err
	}
	if err = temp.Close(); err != nil {
		return nil, err
	}
	if err = os.Rename(temp.Name(), linkPath); err != nil {
		return nil, err
	}
	return &LinkResult{Added: added}, nil
}

func linkImports(dir string) (map[string]bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	result := map[string]bool{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, entry.Name()), nil, parser.ImportsOnly)
		if err != nil {
			return nil, err
		}
		for _, item := range file.Imports {
			path, err := strconv.Unquote(item.Path.Value)
			if err != nil {
				return nil, err
			}
			result[path] = true
		}
	}
	return result, nil
}

func (c *linkCandidate) inspect(path string) error {
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		return err
	}
	if c.packageName != "" && c.packageName != file.Name.Name {
		return fmt.Errorf("selected package contains mixed package clauses: %s", c.dir)
	}
	c.packageName = file.Name.Name
	imports := map[string]string{}
	for _, item := range file.Imports {
		importPath, err := strconv.Unquote(item.Path.Value)
		if err != nil {
			return err
		}
		name := filepath.Base(importPath)
		if item.Name != nil {
			name = item.Name.Name
		}
		imports[name] = importPath
	}
	for _, declaration := range file.Decls {
		switch actual := declaration.(type) {
		case *ast.FuncDecl:
			if actual.Recv == nil && actual.Name.Name == "init" {
				c.hasInit = true
			} else if actual.Recv != nil && len(actual.Recv.List) > 0 {
				receiver := receiverName(actual.Recv.List[0].Type)
				if receiver != "" && (predicateMethod(actual, imports) || codecMethod(actual, imports)) {
					c.holders[receiver] = true
				}
			}
		case *ast.GenDecl:
			for _, item := range actual.Specs {
				switch value := item.(type) {
				case *ast.ValueSpec:
					if actual.Tok != token.VAR {
						continue
					}
					ast.Inspect(value, func(node ast.Node) bool {
						call, ok := node.(*ast.CallExpr)
						if !ok {
							return true
						}
						generic, ok := call.Fun.(*ast.IndexExpr)
						if ok {
							selector, ok := generic.X.(*ast.SelectorExpr)
							if ok && selector.Sel.Name == "TypeFor" {
								qualifier, ok := selector.X.(*ast.Ident)
								if ok && imports[qualifier.Name] == "reflect" {
									if holder, ok := generic.Index.(*ast.Ident); ok {
										c.reachable[holder.Name] = true
									}
								}
							}
						}
						return true
					})
				case *ast.TypeSpec:
					if actual.Tok != token.TYPE {
						continue
					}
					structure, ok := value.Type.(*ast.StructType)
					if !ok {
						continue
					}
					for _, field := range structure.Fields.List {
						if field.Tag == nil || !componentField(field.Type, imports) {
							continue
						}
						tag, tagErr := strconv.Unquote(field.Tag.Value)
						if tagErr != nil {
							return tagErr
						}
						if staticTagValue(tag, "component") != "" {
							c.holders[value.Name.Name] = true
						}
					}
				}
			}
		}
	}
	return nil
}

// staticTagValue reads a Go struct tag from source text without runtime type
// reflection; the input has already been unquoted from the AST literal.
func staticTagValue(tag, key string) string {
	for tag != "" {
		for len(tag) > 0 && tag[0] == ' ' {
			tag = tag[1:]
		}
		if tag == "" {
			break
		}
		colon := strings.IndexByte(tag, ':')
		if colon <= 0 {
			return ""
		}
		name := tag[:colon]
		if len(tag) < colon+2 || tag[colon+1] != '"' {
			return ""
		}
		end := colon + 2
		for end < len(tag) {
			if tag[end] == '\\' {
				end += 2
				continue
			}
			if tag[end] == '"' {
				break
			}
			end++
		}
		if end >= len(tag) {
			return ""
		}
		if name == key {
			value, err := strconv.Unquote(tag[colon+1 : end+1])
			if err != nil {
				return ""
			}
			return value
		}
		tag = tag[end+1:]
	}
	return ""
}

func receiverName(value ast.Expr) string {
	if pointer, ok := value.(*ast.StarExpr); ok {
		value = pointer.X
	}
	if name, ok := value.(*ast.Ident); ok {
		return name.Name
	}
	return ""
}

func namedFromPackage(value ast.Expr, imports map[string]string, packagePath, name string) bool {
	if pointer, ok := value.(*ast.StarExpr); ok {
		value = pointer.X
	}
	selector, ok := value.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != name {
		return false
	}
	qualifier, ok := selector.X.(*ast.Ident)
	return ok && imports[qualifier.Name] == packagePath
}

func named(value ast.Expr, name string) bool {
	identifier, ok := value.(*ast.Ident)
	return ok && identifier.Name == name
}

func anyType(value ast.Expr) bool {
	if named(value, "any") {
		return true
	}
	structure, ok := value.(*ast.InterfaceType)
	return ok && structure.Methods != nil && len(structure.Methods.List) == 0
}

func methodParameters(fn *ast.FuncDecl) []ast.Expr {
	if fn.Type.Params == nil {
		return nil
	}
	var result []ast.Expr
	for _, field := range fn.Type.Params.List {
		count := len(field.Names)
		if count == 0 {
			count = 1
		}
		for range count {
			result = append(result, field.Type)
		}
	}
	return result
}

func predicateMethod(fn *ast.FuncDecl, imports map[string]string) bool {
	if fn.Name.Name != "Compute" || fn.Type.Results == nil || len(fn.Type.Results.List) != 2 {
		return false
	}
	inputs := methodParameters(fn)
	return len(inputs) == 2 && namedFromPackage(inputs[0], imports, "context", "Context") && anyType(inputs[1]) &&
		namedFromPackage(fn.Type.Results.List[0].Type, imports, "github.com/viant/xdatly/predicate", "Criteria") && named(fn.Type.Results.List[1].Type, "error")
}

func codecMethod(fn *ast.FuncDecl, imports map[string]string) bool {
	if fn.Type.Results == nil || len(fn.Type.Results.List) != 2 {
		return false
	}
	inputs := methodParameters(fn)
	if !named(fn.Type.Results.List[1].Type, "error") {
		return false
	}
	switch fn.Name.Name {
	case "New":
		if len(inputs) != 2 || !namedFromPackage(inputs[0], imports, "github.com/viant/xdatly/codec", "Config") {
			return false
		}
		variadic, ok := inputs[1].(*ast.Ellipsis)
		return ok && namedFromPackage(variadic.Elt, imports, "github.com/viant/xdatly/codec", "Option") &&
			namedFromPackage(fn.Type.Results.List[0].Type, imports, "github.com/viant/xdatly/codec", "Instance")
	case "Value":
		if len(inputs) != 3 || !namedFromPackage(inputs[0], imports, "context", "Context") || !anyType(inputs[1]) || !anyType(fn.Type.Results.List[0].Type) {
			return false
		}
		variadic, ok := inputs[2].(*ast.Ellipsis)
		return ok && namedFromPackage(variadic.Elt, imports, "github.com/viant/xdatly/codec", "Option")
	}
	return false
}

func componentField(value ast.Expr, imports map[string]string) bool {
	switch actual := value.(type) {
	case *ast.IndexListExpr:
		value = actual.X
	case *ast.IndexExpr:
		value = actual.X
	}
	selector, ok := value.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Component" {
		return false
	}
	qualifier, ok := selector.X.(*ast.Ident)
	return ok && imports[qualifier.Name] == "github.com/viant/xdatly"
}

const linkSupportHeader = "// Code generated by datly link sync. Additive only.\n"

func (c *linkCandidate) ensureSupport() error {
	if c.packageName == "main" {
		return fmt.Errorf("package main cannot be blank imported")
	}
	missing := make([]string, 0, len(c.holders))
	for holder := range c.holders {
		if c.reachable[holder] {
			continue
		}
		missing = append(missing, holder)
	}
	sort.Strings(missing)
	if len(missing) == 0 && c.hasInit {
		return nil
	}
	path := filepath.Join(c.dir, "datly_link_sync.go")
	content, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		content = []byte(linkSupportHeader + "package " + c.packageName + "\n")
	} else if err != nil {
		return err
	}
	if !strings.HasPrefix(string(content), linkSupportHeader) {
		return fmt.Errorf("existing %s is not owned by link sync", path)
	}
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, path, content, 0)
	if err != nil {
		return err
	}
	if file.Name.Name != c.packageName {
		return fmt.Errorf("link support file has a different package: %s", path)
	}
	alias, hasReflect := "reflect", false
	for _, item := range file.Imports {
		importPath, err := strconv.Unquote(item.Path.Value)
		if err != nil {
			return err
		}
		if importPath == "reflect" {
			hasReflect = true
			if item.Name != nil {
				alias = item.Name.Name
			}
		}
	}
	if len(missing) > 0 && !hasReflect {
		offset := fileSet.File(file.Name.End()).Offset(file.Name.End())
		updated := make([]byte, 0, len(content)+20)
		updated = append(updated, content[:offset]...)
		updated = append(updated, "\n\nimport \"reflect\""...)
		updated = append(updated, content[offset:]...)
		content = updated
	}
	var appended strings.Builder
	for _, holder := range missing {
		fmt.Fprintf(&appended, "\nvar _datlyReachable%s = %s.TypeFor[%s]()\n", holder, alias, holder)
	}
	if !c.hasInit {
		appended.WriteString("\nfunc init() {}\n")
	}
	content = append(content, appended.String()...)
	if _, err := parser.ParseFile(token.NewFileSet(), path, content, 0); err != nil {
		return fmt.Errorf("generated link support is invalid: %w", err)
	}
	return os.WriteFile(path, content, 0o644)
}
