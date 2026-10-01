package compile

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/locking"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
)

func lowerReaderLockCapabilities(parsed *query.Select, root *spec.View) (bool, error) {
	views := canonicalViews(root)
	filtered := make(query.List, 0, len(parsed.List))
	changed := false
	for _, item := range parsed.List {
		call, ok := item.Expr.(*expr.Call)
		if !ok || !strings.EqualFold(strings.TrimSpace(sqlparser.Stringify(call.X)), "row_lock") {
			filtered = append(filtered, item)
			continue
		}
		if item.Alias != "" || (len(call.Args) != 2 && len(call.Args) != 3) {
			return false, fmt.Errorf("row_lock requires a view and a literal physical table/alias without an SQL alias")
		}
		name := strings.ToLower(strings.TrimSpace(sqlparser.Stringify(call.Args[0])))
		view := views[name]
		if view == nil {
			return false, fmt.Errorf("row_lock target view %q does not exist", name)
		}
		target, ok := viewDirectiveValue("row_lock", 1, call.Args[1])
		if !ok {
			return false, fmt.Errorf("row_lock requires a non-empty literal physical table/alias")
		}
		if _, _, err := locking.Target(target); err != nil {
			return false, err
		}
		if view.RowLock != "" {
			return false, fmt.Errorf("row_lock declared more than once for %s", name)
		}
		if len(call.Args) == 3 {
			order, ok := viewDirectiveValue("row_lock", 2, call.Args[2])
			if !ok {
				return false, fmt.Errorf("row_lock order must be a literal physical column")
			}
			if err := locking.OrderTarget(target, order); err != nil {
				return false, err
			}
			view.RowLockOrder = order
		}
		view.RowLock = target
		changed = true
	}
	if changed {
		parsed.List = filtered
	}
	return changed, nil
}
