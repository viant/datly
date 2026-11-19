package locator

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

type dataViewLocatorRecord struct {
	ID int
}

func TestDataView_Value_UsesRequestedScalarTypeToUnwrapSingleResult(t *testing.T) {
	aView := &view.View{
		Name:   "CurFoos",
		Schema: state.NewSchema(reflect.TypeOf([]*dataViewLocatorRecord{})),
	}
	aView.Schema.Cardinality = state.Many
	locator := &DataView{
		Views: view.NamedViews{"CurFoos": aView},
		ReadInto: func(ctx context.Context, dest interface{}, aView *view.View) error {
			target := dest.(*[]*dataViewLocatorRecord)
			*target = append(*target, &dataViewLocatorRecord{ID: 7})
			return nil
		},
	}

	value, ok, err := locator.Value(context.Background(), reflect.TypeOf(&dataViewLocatorRecord{}), "CurFoos")
	require.NoError(t, err)
	require.True(t, ok)
	record, ok := value.(*dataViewLocatorRecord)
	require.True(t, ok)
	require.Equal(t, 7, record.ID)
}

func TestDataView_Value_PreservesSliceForSliceTarget(t *testing.T) {
	aView := &view.View{
		Name:   "CurFoos",
		Schema: state.NewSchema(reflect.TypeOf([]*dataViewLocatorRecord{})),
	}
	aView.Schema.Cardinality = state.Many
	locator := &DataView{
		Views: view.NamedViews{"CurFoos": aView},
		ReadInto: func(ctx context.Context, dest interface{}, aView *view.View) error {
			target := dest.(*[]*dataViewLocatorRecord)
			*target = append(*target, &dataViewLocatorRecord{ID: 7})
			return nil
		},
	}

	value, ok, err := locator.Value(context.Background(), reflect.TypeOf([]*dataViewLocatorRecord{}), "CurFoos")
	require.NoError(t, err)
	require.True(t, ok)
	records, ok := value.([]*dataViewLocatorRecord)
	require.True(t, ok)
	require.Len(t, records, 1)
	require.Equal(t, 7, records[0].ID)
}
