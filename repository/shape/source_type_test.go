package shape

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/x"
)

type sampleShape struct {
	ID int
}

func TestSource_ResolveRootType_FromRegistry(t *testing.T) {
	registry := x.NewRegistry()
	registry.Register(x.NewType(reflect.TypeOf(sampleShape{})))
	src := &Source{
		TypeName:     "github.com/viant/datly/repository/shape.sampleShape",
		TypeRegistry: registry,
	}
	rType, err := src.ResolveRootType()
	require.NoError(t, err)
	require.Equal(t, reflect.TypeOf(sampleShape{}), rType)
}

func TestSource_EnsureTypeRegistry_RegistersRoot(t *testing.T) {
	src := &Source{Struct: &sampleShape{}}
	registry := src.EnsureTypeRegistry()
	require.NotNil(t, registry)
	require.NotEmpty(t, src.TypeName)
	require.NotNil(t, registry.Lookup(src.TypeName))
}
