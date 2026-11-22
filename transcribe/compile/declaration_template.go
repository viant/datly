package compile

import (
	"github.com/viant/datly/internal/sqlanalysis"
	"github.com/viant/datly/transcribe/dql"
)

// DeclarationAnalysisSource reuses reader statement analysis and native
// predicate masking. Its result is metadata only, never executable SQL.
func DeclarationAnalysisSource(source string) (string, error) {
	if len(dql.EmbeddedSQLRefs(source)) != 0 {
		// The resource owner expands these after source discovery. Preserve
		// the existing parser-owned embed declaration path until then.
		return source, nil
	}
	template, err := newReadTemplateSource(source)
	if err != nil {
		return "", err
	}
	return sqlanalysis.DeclarationSQL(template.AnalysisSQL)
}
