package sql

import (
	"strings"
	"testing"
)

func TestSelectorProjectionPreservesPhysicalRowLock(t *testing.T) {
	const source = "SELECT policy.* FROM (SELECT a.ID FROM advertisers a WHERE a.ID = ? FOR UPDATE) policy"
	for _, selected := range [][]string{nil, {"ID"}} {
		result, err := ApplySelectorProjection(source, selected, nil)
		if err != nil {
			t.Fatal(err)
		}
		upper := strings.ToUpper(result)
		if strings.Count(upper, "FOR UPDATE") != 1 || !strings.Contains(upper, "FOR UPDATE)") {
			t.Fatalf("physical lock lost or moved: %s", result)
		}
	}
}
