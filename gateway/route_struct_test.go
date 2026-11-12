package gateway

import (
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

func TestRouterGenerateGoStruct_PreservesLegacyInternalTagSpacing(t *testing.T) {
	schemaType := reflect.StructOf([]reflect.StructField{
		{
			Name: "VendorId",
			Type: reflect.TypeOf((*int)(nil)),
			Tag:  reflect.StructTag(`sqlx:"VENDOR_ID" internal:"true"`),
		},
	})
	schema := &state.Schema{Cardinality: state.Many}
	schema.SetType(reflect.SliceOf(schemaType))

	component := &repository.Component{
		View: &view.View{Schema: schema},
	}

	router := &Router{}
	statusCode, content := router.generateGoStruct(component)

	require.Equal(t, http.StatusOK, statusCode)
	require.True(t, strings.Contains(string(content), `sqlx:"VENDOR_ID"  internal:"true"`), string(content))
}
