package generate

import (
	"errors"
	"fmt"
	"github.com/viant/datly/constant/identifier"
	"github.com/viant/velty/ast"
	vexpr "github.com/viant/velty/ast/expr"
	"github.com/viant/velty/ast/stmt"
	vparser "github.com/viant/velty/parser"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
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
	// Original generated SQL can retain explicit query-value binding calls.
	// The runtime template compiler binds direct emissions of those same typed
	// query inputs. Compare those existing forms without rewriting saved SQL.
	comparisonOriginal := r.queryBindingComparison(original)
	comparisonCanonical := r.queryBindingComparison(canonical)
	if strings.TrimSpace(comparisonOriginal) == strings.TrimSpace(comparisonCanonical) {
		return original, nil
	}
	parsed, err := sqlparser.ParseQuery(comparisonCanonical)
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
				if strings.TrimSpace(inner) == strings.TrimSpace(sqltext.MaskLineComments(comparisonOriginal)) {
					return original, nil
				}
			}
		}
	}
	return "", fmt.Errorf("linked SQL resource %s:%s differs from application-owned embedded filesystem beyond its canonical derived source wrapper", pkg, path)
}

// queryBindingComparison uses the template compiler's existing AST spans and
// local-variable authority. Only emitted query values have binding equivalence;
// template decisions, assignments, SQL literals and shadowed locals do not.
func (r *planResolver) queryBindingComparison(source string) string {
	if r.input.Component == nil || !strings.Contains(source, "$criteria.AppendBinding($Unsafe.") {
		return source
	}
	root, spans, err := vparser.ParseWithSpansDetailed([]byte(source))
	if err != nil {
		return source
	}
	locals := map[string]bool{}
	identifier.CollectLocalVariables(root.Statements(), locals)
	bindings := map[string]string{}
	for _, parameter := range r.input.Component.Parameters {
		if parameter != nil && parameter.Source.Kind == "query" && parameter.Name != "" && !locals[parameter.Name] {
			bindings["$criteria.AppendBinding($Unsafe."+parameter.Name+")"] = "$" + parameter.Name
		}
	}
	replacements := map[int]string{}
	ends := map[int]int{}
	var collect func([]ast.Statement)
	collect = func(statements []ast.Statement) {
		for _, statement := range statements {
			switch actual := statement.(type) {
			case *vexpr.Select:
				span, ok := spans[actual]
				end := span.End + 1
				if !ok || span.Start < 0 || end > len(source) || end <= span.Start || sqltext.ProtectionAt(source, span.Start) != "" {
					continue
				}
				if replacement := bindings[source[span.Start:end]]; replacement != "" {
					replacements[span.Start], ends[span.Start] = replacement, end
				}
			case *stmt.If:
				collect(actual.Body.Statements())
				for branch := actual.Else; branch != nil; branch = branch.Else {
					collect(branch.Body.Statements())
				}
			case *stmt.ForEach:
				collect(actual.Statements())
			case *stmt.ForLoop:
				collect(actual.Statements())
			}
		}
	}
	collect(root.Statements())
	starts := make([]int, 0, len(replacements))
	for start := range replacements {
		starts = append(starts, start)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(starts)))
	for _, start := range starts {
		source = source[:start] + replacements[start] + source[ends[start]:]
	}
	return source
}
