package generate

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	"github.com/viant/datly/transcribe/compile"
	xshape "github.com/viant/x/shape"
)

func TestDQLRowLockMetadataGeneratedBootstrapRoundTrip(t *testing.T) {
	view, err := compile.NewReader().Compile(compile.ReadInput{View: &spec.View{Name: "Rows"}, SQL: `SELECT r.id,row_lock(r,'records r','r.id') FROM records r`})
	require.NoError(t, err)
	encoded, err := appendViewTags(`parameter:"Rows,kind=view,in=Rows"`, view)
	require.NoError(t, err)
	input, err := (xshape.Runtime{}).Struct([]xshape.RuntimeField{{Name: "Rows", Type: reflect.TypeOf([]struct{ ID int }{}), Tag: reflect.StructTag(encoded)}})
	require.NoError(t, err)
	component, err := (&bootstrap.RouteSource{PackagePath: "example.com/project", FieldName: "Contract", Tag: tag.Component{Name: "Rows", Path: "/rows", Method: "GET"}}).Resolve(input, reflect.TypeOf(struct{}{}))
	require.NoError(t, err)
	require.Len(t, component.Views, 1)
	require.Equal(t, "records r", component.Views[0].RowLock)
	require.Equal(t, "r.id", component.Views[0].RowLockOrder)
	require.Equal(t, view.RowLock, view.Clone().RowLock)
	require.Equal(t, view.RowLockOrder, view.Clone().RowLockOrder)
}
