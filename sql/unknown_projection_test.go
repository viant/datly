package sql

import (
	"errors"
	"fmt"
	"github.com/viant/datly/data"
	"testing"
)

func TestRequestedUnknownProjectionColumnIsTyped(t *testing.T) {
	for _, source := range []string{"SELECT id,name FROM records", "SELECT r.* FROM (SELECT id,name FROM records) r", "SELECT * FROM records"} {
		view := &data.View{Columns: []*data.Column{{Name: "id", Column: "id"}, {Name: "name", Column: "name"}}}
		_, err := ApplySelectorProjection(source, []string{"notThere"}, view)
		var unknown *UnknownProjectionColumnError
		if !errors.As(fmt.Errorf("reader: %w", err), &unknown) || unknown.Column != "notthere" || unknown.Error() != "not found column notthere" {
			t.Fatalf("source%s err%v", source, err)
		}
	}
}
func TestUnresolvedAuthoredProjectionRemainsDifferent(t *testing.T) {
	_, err := ApplySelectorProjection("not valid SQL", nil, &data.View{})
	var unknown *UnknownProjectionColumnError
	if err == nil || errors.As(err, &unknown) {
		t.Fatalf("static projection incorrectly classified%v", err)
	}
}
