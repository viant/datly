package program

import (
	"fmt"
	"strings"

	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/transcribe/dql/statement"
)

// BufferedStatement identifies a standalone typed service call and rejects
// competing handlers or statements. An optional read supplies its result shape.
func BufferedStatement(prepared *dql.PreparedSource, hasHandler bool) (string, error) {
	statementProgram := ""
	for _, item := range prepared.Statements {
		// Preserve existing Velty programs, including their unqualified argument
		// names and assignments. The typed statement product uses explicit
		// canonical Input/Output arguments and is one standalone service call.
		if strings.EqualFold(item.Operation, "Execute") && (len(prepared.Statements) != 1 || !BufferedExecute(prepared.SQL[item.Start:item.End])) {
			continue
		}
		batchStatement := (strings.EqualFold(item.Operation, "Insert") || strings.EqualFold(item.Operation, "Delete") || strings.EqualFold(item.Operation, "InsertIgnore")) && BufferedBatch(prepared.SQL[item.Start:item.End])
		if item.Kind == statement.KindService && (batchStatement || (strings.EqualFold(item.Operation, "Update") && BufferedUpdate(prepared.SQL[item.Start:item.End])) || strings.EqualFold(item.Operation, "ExecuteWithResult") || strings.EqualFold(item.Operation, "ExecuteWithOptionalResult") || strings.EqualFold(item.Operation, "Execute") || strings.EqualFold(item.Operation, "Merge")) {
			shapeOnly := len(prepared.Statements) == 2
			if shapeOnly {
				for _, other := range prepared.Statements {
					if other != item && other.Kind != statement.KindRead {
						shapeOnly = false
					}
				}
			}
			if statementProgram != "" || hasHandler || (len(prepared.Statements) != 1 && !shapeOnly) {
				return "", fmt.Errorf("buffered result statement cannot be combined with another handler or authored statement")
			}
			statementProgram = strings.TrimSpace(prepared.SQL[item.Start:item.End])
		}
	}
	return statementProgram, nil
}
