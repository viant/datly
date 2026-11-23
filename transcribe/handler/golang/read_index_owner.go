package golang

import (
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	xmodule "github.com/viant/x/module"
	xshape "github.com/viant/x/shape"
)

// Transitive project imports can close the same cycle as a direct row import.
type indexImportGraph struct {
	owner, module, directory string
	imports                  map[string][]string
}

func newIndexImportGraph(root, owner string) (*indexImportGraph, error) {
	graph := &indexImportGraph{owner: owner, imports: map[string][]string{}}
	if root != "" {
		module, err := xmodule.LocateLocal(root)
		if err != nil {
			return nil, err
		}
		graph.module, graph.directory = module.Path, module.Dir
	}
	return graph, nil
}

func (g *indexImportGraph) reachesOwner(pkg string, visited map[string]bool) (bool, error) {
	if pkg == g.owner {
		return true, nil
	}
	if visited[pkg] || g.module == "" || (pkg != g.module && !strings.HasPrefix(pkg, g.module+"/")) {
		return false, nil
	}
	visited[pkg] = true
	imports, ok := g.imports[pkg]
	if !ok {
		dir := filepath.Join(g.directory, filepath.FromSlash(strings.TrimPrefix(strings.TrimPrefix(pkg, g.module), "/")))
		p, err := build.Default.ImportDir(dir, 0)
		if err != nil {
			if _, empty := err.(*build.NoGoError); !empty {
				// ImportDir does not wrap ErrNotExist for a missing directory.
				// A fresh generation can reference its model before it exists.
				if _, statErr := os.Stat(dir); os.IsNotExist(statErr) {
					return false, nil
				} else if statErr != nil {
					return false, statErr
				}
				return false, err
			}
		}
		if p != nil {
			imports = p.Imports
		}
		g.imports[pkg] = imports
	}
	for _, next := range imports {
		if hit, err := g.reachesOwner(next, visited); hit || err != nil {
			return hit, err
		}
	}
	return false, nil
}

func (g *indexImportGraph) ownerCollection(l *lowerer, read plan.ReadCollection) (bool, error) {
	refs := []spec.TypeRef{read.Type}
	for _, f := range read.Fields {
		refs = append(refs, f.Type)
	}
	for _, k := range read.Keys {
		refs = append(refs, k.Type)
	}
	for _, group := range read.Groups {
		for _, k := range group.Parts {
			refs = append(refs, k.Type)
		}
	}
	visited := map[string]bool{}
	for _, ref := range refs {
		var hit bool
		// Inspect the canonical type without allocating an import alias. Probing
		// a collection must not change the output of an already-valid layout.
		canonical, err := (xshape.Resolver{Package: ref.Package, Imports: l.pathsByAlias}).Canonical(ref.Name)
		if err != nil {
			return false, err
		}
		resolver := xshape.Resolver{Rewriter: func(name string) (string, error) {
			if hit {
				return name, nil
			}
			r, err := (xshape.Resolver{}).Reference(name)
			if err != nil {
				return "", err
			}
			if r.Qualifier != "" {
				hit, err = g.reachesOwner(r.Qualifier, visited)
			}
			return name, err
		}}
		_, err = resolver.Rewrite(canonical)
		if hit || err != nil {
			return hit, err
		}
	}
	return false, nil
}

type ownerIndexLoops struct {
	unique, group ast.Expr
	declarations  []ast.Decl
}

func newOwnerIndexLoops(owner, helper *lowerer) (*ownerIndexLoops, error) {
	// Factories are unique only within their defining package.
	hash := sha256.Sum256([]byte(owner.config.PackagePath + "\x00" + owner.factory))
	prefix := fmt.Sprintf("%sLookup%x", strings.TrimPrefix(owner.factory, "New"), hash[:8])
	const source = `package lookup
func Unique[M ~map[K]*R, K comparable, R any](rows []*R, keyOf func(*R) (K, bool), duplicate string) (M, error) {
	result := make(M)
	for _, row := range rows {
		key, ok := keyOf(row)
		if !ok { continue }
		if _, exists := result[key]; exists { return nil, fmt.Errorf("%s", duplicate) }
		result[key] = row
	}
	return result, nil
}
func Group[M ~map[K][]*R, K comparable, R any](rows []*R, keyOf func(*R) (K, bool)) M {
	result := make(M)
	for _, row := range rows {
		if key, ok := keyOf(row); ok { result[key] = append(result[key], row) }
	}
	return result
}`
	file, err := parser.ParseFile(token.NewFileSet(), "", source, 0)
	if err != nil {
		return nil, err
	}
	result := &ownerIndexLoops{declarations: file.Decls}
	for _, decl := range file.Decls {
		fn := decl.(*ast.FuncDecl)
		unique := fn.Name.Name == "Unique"
		fn.Name.Name = prefix + fn.Name.Name
		ast.Inspect(fn, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && id.Name == "fmt" {
				id.Name = helper.fmtAlias
			}
			return true
		})
		ref, err := owner.typeReference(spec.TypeRef{Package: helper.config.PackagePath, Name: fn.Name.Name})
		if err != nil {
			return nil, err
		}
		if unique {
			result.unique = ref
		} else {
			result.group = ref
		}
	}
	return result, nil
}

func (l *ownerIndexLoops) call(grouped bool, typ, key, method string) ast.Expr {
	fn := l.group
	args := []ast.Expr{ast.NewIdent("rows"), ast.NewIdent(key)}
	if !grouped {
		fn = l.unique
		args = append(args, &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote("ambiguous application index " + method)})
	}
	return callExpr(&ast.IndexExpr{X: fn, Index: ast.NewIdent(typ)}, args...)
}
