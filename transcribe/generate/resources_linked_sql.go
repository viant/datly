package generate

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	sqltext "github.com/viant/sqlparser/source"
)

// A linked row's relative SQL URI belongs to its existing Go package. A
// canonical DQL source may add a transparent scope wrapper for generated rows;
// that wrapper cannot replace the SQL used by an unchanged linked row's on tag.
func (r *planResolver) linkedSQLResource(pkg, path, canonical string) (string, error) {
	if r.input.ProjectRoot == "" {
		return canonical, nil
	}
	authority, err := typecatalog.NewDestinationAuthority(r.input.ProjectRoot)
	if err != nil {
		return "", err
	}
	destination, err := authority.Resolve(pkg, "")
	if err != nil {
		return "", err
	}
	root, err := os.OpenRoot(filepath.Join(r.input.ProjectRoot, destination.Directory))
	if errors.Is(err, fs.ErrNotExist) {
		return canonical, nil
	}
	if err != nil {
		return "", err
	}
	defer root.Close()
	body, err := fs.ReadFile(root.FS(), path)
	if errors.Is(err, fs.ErrNotExist) {
		return canonical, nil
	}
	if err != nil {
		return "", err
	}
	original := string(body)
	if strings.TrimSpace(original) == strings.TrimSpace(canonical) {
		return original, nil
	}
	parsed, err := sqlparser.ParseQuery(canonical)
	if err == nil && parsed != nil && len(parsed.List) == 1 && len(parsed.Joins) == 0 && len(parsed.WithSelects) == 0 && !parsed.WithRecursive && parsed.Union == nil && len(parsed.GroupBy) == 0 && parsed.Having == nil && len(parsed.OrderBy) == 0 && parsed.Limit == nil && parsed.Offset == nil && parsed.Qualify == nil && parsed.QualifyClause == nil && parsed.Kind == "" && parsed.Window == nil {
		if star, ok := parsed.List[0].Expr.(*expr.Star); ok && len(star.Except) == 0 && parsed.List[0].Alias == "" {
			if star.X != nil {
				qualifier, ok := star.X.(*expr.Ident)
				if !ok || qualifier.Name != "*" {
					return "", fmt.Errorf("linked SQL resource %s:%s has a qualified canonical wildcard", pkg, path)
				}
			}
			if raw, ok := parsed.From.X.(*expr.Raw); ok {
				inner := strings.TrimSpace(raw.Raw)
				if group, end, ok := sqltext.ReadGroupString(inner, 0, '(', ')'); ok && end == len(inner) {
					inner = strings.TrimSpace(group[1 : len(group)-1])
				}
				if strings.TrimSpace(inner) == strings.TrimSpace(original) {
					return original, nil
				}
			}
		}
	}
	return "", fmt.Errorf("linked SQL resource %s:%s differs from application-owned embedded filesystem beyond its canonical derived source wrapper", pkg, path)
}
