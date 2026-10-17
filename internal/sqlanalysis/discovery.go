package sqlanalysis

import (
	"fmt"
	"strings"

	"github.com/viant/parsly"
	"github.com/viant/sqlparser"
	sqlsource "github.com/viant/sqlparser/source"
	veltyexpr "github.com/viant/velty/ast/expr"
	veltyparser "github.com/viant/velty/parser"
)

// DiscoveryPredicates prepares only runtime tuple predicates for metadata
// discovery. Their arguments may call generated input methods that do not exist
// on a provisional input shape. Validate predicate placement without executing
// those methods; the caller retains the original SQL for runtime evaluation.
func DiscoveryPredicates(source string) (string, error) {
	prefix := "__datly_discovery_predicate_"
	for strings.Contains(source, prefix) {
		prefix += "_"
	}
	markers := map[string]string{}
	var result strings.Builder
	previous := 0
	input := []byte(source)
	scanner := sqlsource.NewCodeScanner(source, 0)
	for pos, ok := scanner.Next(); ok; pos, ok = scanner.Next() {
		if pos < previous || !(strings.HasPrefix(source[pos:], "$criteria.") || strings.HasPrefix(source[pos:], "${criteria.")) {
			continue
		}
		cursor := parsly.NewCursor("", input, 0)
		cursor.Pos = pos + 1
		selector, err := veltyparser.MatchSelector(cursor)
		if err != nil {
			return "", fmt.Errorf("malformed discovery predicate at byte %d: %w", pos, err)
		}
		method, ok := selector.X.(*veltyexpr.Select)
		if selector.ID != "criteria" || !ok || method.ID != "CompositeIn" {
			continue
		}
		call, ok := method.X.(*veltyexpr.Call)
		if !ok || len(call.Args) != 2 || call.X != nil {
			return "", fmt.Errorf("discovery criteria.CompositeIn requires exactly two arguments and no chained access")
		}
		marker := fmt.Sprintf("%s%d", prefix, len(markers))
		markers[marker] = ""
		result.WriteString(source[previous:pos])
		result.WriteString("(" + marker + " = 1)")
		previous = cursor.Pos
	}
	if len(markers) == 0 {
		return source, nil
	}
	result.WriteString(source[previous:])
	masked := result.String()
	parsed, err := sqlparser.ParseQuery(unwrapAnalysisSQL(masked), sqlparser.WithStructuralValidation())
	if err != nil {
		return "", fmt.Errorf("parse discovery predicate scope: %w", err)
	}
	if err := validateAnalysisPredicates(parsed, markers, nil); err != nil {
		return "", err
	}
	for marker := range markers {
		masked = strings.ReplaceAll(masked, "("+marker+" = 1)", "(1 = 0)")
	}
	return masked, nil
}
