package column

import (
	"strings"
	"testing"
)

func TestStaticProjectionRetainsParentMacroRuntimeOwnership(t *testing.T) {
	source := `SELECT e.id FROM events e WHERE 1=1 $View.ParentJoinOn("AND","e.id")`
	analysis, err := projectionAnalysisSQL(source)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(analysis, "ParentJoinOn") || !strings.Contains(source, `$View.ParentJoinOn("AND","e.id")`) {
		t.Fatal("analysis/runtime ownership mismatch")
	}
	if _, err = projectionAnalysisSQL(`SELECT e.id FROM events e $View.ParentJoinOn("WHERE","e.id OR 1=1")`); err == nil {
		t.Fatal("invalid parent key accepted")
	}
	quoted := `SELECT '$View.ParentJoinOn("WHERE","e.id")' AS literal FROM events e`
	analysis, err = projectionAnalysisSQL(quoted)
	if err != nil || analysis != quoted {
		t.Fatalf("protected SQL changed: %s %v", analysis, err)
	}
}
