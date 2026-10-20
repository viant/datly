package readergraph

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/x"
	"reflect"
	"testing"
)

func TestReaderMetadataSlicePointerIdentity(t *testing.T) {
	type useCase struct {
		description           string
		input                 reflect.Type
		pointer, slicePointer bool
		expectedError         bool
	}
	for _, tc := range []useCase{
		{"slice", reflect.TypeFor[[]uint8](), false, false, false},
		{"pointer to slice", reflect.TypeFor[*[]uint8](), false, true, false},
		{"slice of pointers", reflect.TypeFor[[]*uint8](), true, false, false},
		{"pointer to slice of pointers", reflect.TypeFor[*[]*uint8](), true, true, false},
		{"distinct pointer positions", reflect.TypeFor[*[]uint8](), true, false, true},
	} {
		t.Run(tc.description, func(t *testing.T) {
			row := x.NewType(reflect.StructOf([]reflect.StructField{{Name: "Data", Type: tc.input}}), x.WithName("Row"), x.WithPkgPath("example.com/reader"))
			document := documentFixture()
			document.Component.RootView.TypeName = row.Key()
			identity, err := document.Component.RootView.Identity()
			require.NoError(t, err)
			document.Rows = map[string]string{identity: row.Key()}
			document.Component.RootView.Columns = []*spec.Column{{Name: "Data", Type: spec.TypeRef{Name: "uint8", Cardinality: spec.CardinalityMany, Pointer: tc.pointer, SlicePointer: tc.slicePointer}}}
			err = CheckRows(&document, func(name string) (*x.Type, error) {
				if name == row.Key() {
					return row, nil
				}
				return nil, nil
			})
			if tc.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
