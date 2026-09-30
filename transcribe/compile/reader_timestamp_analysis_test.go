package compile

import (
	"strings"
	"testing"
)

func TestTimestampScalarAnalysisPreservesInputColumnAuthority(t *testing.T) {
	source := `#if($Enabled)
AND ${View.TimestampSecondsUTC("COALESCE(t.completed_at,t.updated_at)")} > $After
ORDER BY ${View.TimestampNanoseconds("t.created_at")}
#end`
	analysis, err := newReadTemplateSource(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"CAST(COALESCE(t.completed_at,t.updated_at) AS CHAR)", "CAST(t.created_at AS BIGINT)"} {
		if !strings.Contains(analysis.AnalysisSQL, expected) {
			t.Fatalf("column/type authority absent from analysis: %s", analysis.AnalysisSQL)
		}
	}
	for _, call := range []string{`${View.TimestampSecondsUTC($Injected)}`, `${View.TimestampSecondsUTC("t.created_at;DELETE FROM records")}`, `${View.TimestampNanoseconds("NOW()")}`} {
		if _, err = newReadTemplateSource("#if($Enabled)\nAND " + call + " > 0\n#end"); err == nil {
			t.Fatalf("dynamic or unbounded SQL renderer accepted: %s", call)
		}
	}
}
