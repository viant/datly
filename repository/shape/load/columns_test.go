package load

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

type sampleInferredColumnsRoot struct {
	ID       int                  `sqlx:"ID"`
	Products []*sampleInferredRel `view:",table=PRODUCT" on:"Id:ID=VendorId:VENDOR_ID"`
	Ignored  string               `sqlx:"-"`
}

type sampleInferredRel struct {
	VendorID int `sqlx:"VENDOR_ID"`
}

func TestInferColumnsFromType_SkipsSemanticFields(t *testing.T) {
	cols := inferColumnsFromType(reflect.TypeOf(sampleInferredColumnsRoot{}))
	require.Len(t, cols, 1)
	require.Equal(t, "ID", cols[0].Name)
}
