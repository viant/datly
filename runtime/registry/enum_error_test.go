package registry

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindinput "github.com/viant/bindly/input"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/tagly/choice"
	"github.com/viant/xdatly/response"
)

func TestEnumBindingDecodeFailureRetainsClientStatus(t *testing.T) {
	for _, tc := range []struct {
		name   string
		typeOf reflect.Type
		tag    reflect.StructTag
		value  any
	}{
		{"boolean", reflect.TypeFor[int](), `choice:"0:false" choice:"1:true" enumAccept:"boolean" enumStyle:"boolean"`, "invalid"},
		{"numeric", reflect.TypeFor[int](), `choice:"0:ASAP" choice:"1:EVEN" enumAccept:"literal"`, "invalid"},
		{"collection", reflect.TypeFor[[]string](), `mcpEnum:"true" choice:"DISPLAY" choice:"VIDEO" enumAccept:"literal"`, []string{"invalid"}},
		{"null", reflect.TypeFor[int](), `choice:"0:ASAP" enumAccept:"literal" enumNull:"reject"`, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binding, err := EnumBinding(bindly.BindingSpec{Location: bindstate.Location{Kind: "query", In: "period"}}, reflect.StructField{Name: "Period", Type: tc.typeOf, Tag: tc.tag})
			require.NoError(t, err)
			_, err = binding.Transformer.Transform(t.Context(), nil, tc.value)
			require.Error(t, err)
			var inputError *bindinput.Error
			require.ErrorAs(t, err, &inputError)
			require.NotNil(t, inputError.Cause)
			wrapped := &bindly.BindingError{Path: "Period", Cause: err}
			require.Equal(t, http.StatusBadRequest, response.ErrorStatusCode(wrapped, http.StatusInternalServerError))
			require.ErrorIs(t, wrapped, inputError.Cause)
			if tc.name == "null" {
				require.ErrorIs(t, wrapped, choice.ErrNullInput)
			}
		})
	}
}

type failedEnumMarshal struct{ cause error }

func (v failedEnumMarshal) MarshalJSON() ([]byte, error) { return nil, v.cause }

func TestEnumBindingDoesNotReclassifySourceMarshalFailures(t *testing.T) {
	binding, err := EnumBinding(bindly.BindingSpec{Location: bindstate.Location{Kind: "query"}}, reflect.StructField{Name: "Mode", Type: reflect.TypeFor[int](), Tag: `choice:"0:ASAP" enumAccept:"literal"`})
	require.NoError(t, err)
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, errors.New("source unavailable")} {
		_, err := binding.Transformer.Transform(t.Context(), nil, failedEnumMarshal{cause: cause})
		require.ErrorIs(t, err, cause)
		var inputError *bindinput.Error
		require.False(t, errors.As(err, &inputError))
	}
}
