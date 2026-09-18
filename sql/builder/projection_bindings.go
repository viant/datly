package builder

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/viant/sqlx"
)

// nameProjectionBindings gives each positional value a stable identity while
// SQL is rewritten. Values never enter SQL text; the ordinary binder still
// emits executable placeholders and expands slice values.
func nameProjectionBindings(source string, args []any, resolver sqlx.ParameterResolver) (string, sqlx.ParameterResolver, error) {
	parameters := sqlx.ParseParameters(source)
	count := parameters.PositionalCount()
	if count > len(args) {
		return "", nil, fmt.Errorf("missing positional SQL argument %d", len(args)+1)
	}
	if count < len(args) {
		return "", nil, fmt.Errorf("SQL consumed %d positional arguments but %d were supplied", count, len(args))
	}
	if count == 0 {
		return source, resolver, nil
	}
	prefix := "__datly_projection_arg_"
	for strings.Contains(strings.ToLower(source), prefix) {
		prefix = "_" + prefix
	}
	values := make(map[string]any, count)
	rewritten := parameters.RewritePositional(func(index int) string {
		name := prefix + strconv.Itoa(index)
		values[name] = args[index]
		return ":" + name
	})
	return rewritten, func(name string) (any, bool, error) {
		if value, ok := values[name]; ok {
			return value, true, nil
		}
		if resolver == nil {
			return nil, false, fmt.Errorf("parameter resolver is required for named placeholder %s", name)
		}
		return resolver(name)
	}, nil
}
