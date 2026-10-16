package column

import (
	"github.com/viant/datly/internal/sqlanalysis"
	"github.com/viant/sqlparser/query"
)

// projectionAnalysisSQL uses the shared static analyzer with this transcription's
// declared-input authority. It never evaluates runtime template expressions.
func projectionAnalysisSQL(SQL string, inputs ...*analysisInputs) (string, error) {
	var validate sqlanalysis.InputValidator
	if len(inputs) > 0 && inputs[0] != nil {
		validate = inputs[0].validate
	}
	return sqlanalysis.SQL(SQL, validate)
}

func parseProjectionAnalysis(SQL string) (*query.Select, error) {
	_, parsed, err := sqlanalysis.Parse(SQL, nil)
	return parsed, err
}
