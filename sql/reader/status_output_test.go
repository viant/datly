package reader

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/xdatly/response"
)

func TestStatusOutputAccessors(t *testing.T) {
	type namedStatus string
	type scalar struct{ State namedStatus }
	type embedded struct{ response.Status }
	type pointer struct{ State *response.Status }
	for _, tc := range []struct {
		name  string
		field string
		out   any
		want  any
	}{
		{"scalar", "State", &scalar{}, &scalar{State: "ok"}},
		{"embedded", "Status", &embedded{response.Status{Message: "retained"}}, &embedded{response.Status{Status: "ok", Message: "retained"}}},
		{"pointer", "State", &pointer{}, &pointer{State: &response.Status{Status: "ok"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &outputAccessors{}
			require.NoError(t, a.compileStatus(tc.field, reflect.TypeOf(tc.out)))
			require.NoError(t, a.writeSuccess(tc.out))
			require.Equal(t, tc.want, tc.out)
		})
	}
	a := &outputAccessors{}
	require.NoError(t, a.compileStatus("", reflect.TypeOf(struct{}{})))
	require.NoError(t, a.writeSuccess(&struct{}{}))
	require.Error(t, a.compileStatus("Status", reflect.TypeOf(struct{ Status int }{})))
}
