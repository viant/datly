package locator

import (
	"context"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	hstate "github.com/viant/xdatly/handler/state"
)

func TestForm_Value_PreservesRepeatedQueryValues(t *testing.T) {
	req := httptest.NewRequest("GET", "http://localhost/test?site_id=1&site_id=2&site_id=3", nil)
	locator := &Form{
		form:    hstate.NewForm(),
		request: req,
	}

	value, ok, err := locator.Value(context.Background(), reflect.TypeOf([]int{}), "site_id")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []string{"1", "2", "3"}, value)
}
