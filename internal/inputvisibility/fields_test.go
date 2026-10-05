package inputvisibility

import (
	"reflect"
	"testing"

	xshape "github.com/viant/x/shape"
)

type privateFields struct{ Secret string }
type visibilityRecord struct {
	privateFields `internal:"true"`
	Hidden        bool `internal:"true"`
	Public        int  `internal:"false"`
}

func TestNativeSelectedFieldsHonorEmbeddingOwnership(t *testing.T) {
	owner := reflect.TypeFor[visibilityRecord]()
	fields, err := xshape.Linked(owner).JSONFields()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"Secret": true, "Hidden": true, "Public": false}
	for _, field := range fields {
		if Internal(owner, field.Field.Index) != want[field.Name] {
			t.Fatalf("visibility differs for %s", field.Name)
		}
		delete(want, field.Name)
	}
	if len(want) != 0 {
		t.Fatalf("native fields missing: %v", want)
	}
}
