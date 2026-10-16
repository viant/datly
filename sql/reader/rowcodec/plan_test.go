package rowcodec

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"reflect"
	"testing"
)

type scalarCaption string
type contractScanner int

func (*contractScanner) Scan(any) error { return nil }

func TestScalarScanPreservesScannerAndEncodingContracts(t *testing.T) {
	type row struct {
		Ordinary int             `sqlx:"ordinary"`
		Scanned  contractScanner `sqlx:"scanned"`
		Encoded  scalarCaption   `sqlx:"encoded,enc=JSON"`
	}
	plan, err := Compile(Config{RowType: reflect.TypeFor[row](), Columns: []*data.Column{{Name: "Ordinary"}, {Name: "Scanned"}, {Name: "Encoded"}}})
	require.NoError(t, err)
	require.NotNil(t, plan)
	raw, ok := plan.scanType.FieldByName("Raw")
	require.True(t, ok)
	require.Equal(t, 1, raw.Type.NumField(), "only ordinary scalar gets a nullable scan slot")
	require.Equal(t, reflect.TypeFor[*int](), raw.Type.Field(0).Type)
}
