package transcribe

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/transcribe/internal/link"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
	smodel "github.com/viant/x/syntetic/model"
)

// Output DTOs are needed to plan the complete owner contract, but must never
// participate in fresh SQL discovery. Keep this authority separate until the
// fresh component has compiled; use the same captured declarations on refresh.
type borrowedOutputDependencies struct {
	catalog *typecatalog.Catalog
	names   map[string]bool
}

func borrowedAuthoredOutputs(source *Source, prepared *dql.PreparedSource, directory string, full *typecatalog.Catalog) (*borrowedOutputDependencies, error) {
	result := &borrowedOutputDependencies{catalog: typecatalog.NewCatalog(), names: map[string]bool{}}
	if prepared.TypeContext == nil || source.dependencySnapshot == nil {
		return result, nil
	}
	owner := prepared.TypeContext.PackagePath
	snapshot := source.dependencySnapshot
	pkg := snapshot.Packages[owner]
	if pkg == nil {
		return result, nil
	}
	// The existing loader has already applied build selection and sealed the bytes.
	authored := map[string]*smodel.Type{}
	for _, file := range pkg.Files {
		for _, sealed := range snapshot.Files {
			if filepath.Base(sealed.Path) != filepath.Base(file.Name) || astGeneratedSource(string(sealed.Bytes)) {
				continue
			}
			// Match the complete package directory, not a basename in another package.
			if filepath.Clean(sealed.Path) != filepath.Clean(filepath.Join(directory, filepath.Base(file.Name))) {
				continue
			}
			for _, typ := range file.Types {
				if typ != nil && typ.PkgPath == owner {
					authored[typ.Name] = typ
				}
			}
		}
	}
	resolver, err := typecatalog.NewResolver(full, typecatalog.TranscribeAuthority, link.TypeContext(source.Scope, "", prepared.TypeContext))
	if err != nil {
		return nil, err
	}
	var visit func(string) error
	visit = func(key string) error {
		if !strings.HasPrefix(key, owner+".") {
			return nil
		}
		name := strings.TrimPrefix(key, owner+".")
		if result.names[name] {
			return nil
		}
		typ := authored[name]
		if typ == nil {
			return nil
		} // Generated row/Has declarations retain DQL authority.
		result.names[name] = true
		imports := map[string]string{}
		for alias, imp := range typ.Imports {
			if imp != nil {
				imports[alias] = imp.Path
			}
		}
		body, err := (xshape.Resolver{Package: owner, Imports: imports}).Canonical(typ.Body())
		if err != nil {
			return err
		}
		if _, err = (xshape.Resolver{Rewriter: func(ref string) (string, error) { return ref, visit(ref) }}).Rewrite(body); err != nil {
			return err
		}
		descriptor, ok, err := full.Resolve(typecatalog.TranscribeAuthority, key)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("borrow_sql_row authored output dependency missing: %s", key)
		}
		return result.catalog.Register(typecatalog.TypeOriginPackage, descriptor)
	}
	for _, param := range prepared.Directives.Params {
		if param == nil || (!param.EmitOutput && !strings.EqualFold(param.Source.Kind, "output")) {
			continue
		}
		// Output(view) is a SQL row projection, not authored DTO authority.
		if param.IsDerivedOutput() || strings.EqualFold(param.Source.Name, "view") || strings.EqualFold(param.Source.Kind, "body") || strings.EqualFold(param.Source.Kind, "view") {
			continue
		}
		expression := param.OutputTypeExpr
		if expression == "" {
			expression = param.TypeExpr
		}
		if expression == "" || expression == "?" {
			continue
		}
		canonical, err := resolver.CanonicalDeclaration(expression, owner)
		if err != nil {
			return nil, err
		}
		if _, err = (xshape.Resolver{Rewriter: func(key string) (string, error) { return key, visit(key) }}).Rewrite(canonical); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (d *borrowedOutputDependencies) apply(result *Result) error {
	if d == nil || len(d.names) == 0 {
		return nil
	}
	catalog, err := result.Source.Types.Clone()
	if err != nil {
		return err
	}
	registry, err := d.catalog.Registry(typecatalog.TranscribeAuthority)
	if err != nil {
		return err
	}
	registry.ForEach(func(key string, typ *x.Type) bool {
		// Never replace a type produced by the fresh DQL graph.
		if _, exists, resolveErr := catalog.Resolve(typecatalog.TranscribeAuthority, key); resolveErr != nil {
			err = resolveErr
			return false
		} else if exists {
			return true
		}
		err = catalog.Register(typecatalog.TypeOriginPackage, typ)
		return err == nil
	})
	if err != nil {
		return err
	}
	result.TypeResolver, err = typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, result.TypeContext)
	return err
}
