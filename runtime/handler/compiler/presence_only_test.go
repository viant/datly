package compiler

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/bindly/provider/body"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

type stagedOrdinaryBody struct {
	ID int `json:"id"`
}
type stagedNativeInput struct {
	Request  *stagedOrdinaryBody                        `parameter:"Request,kind=body"`
	Supplied bool                                       `parameter:"Supplied,kind=body,in=channelV2,presenceOnly=true" internal:"true"`
	Channel  int                                        `parameter:"Channel,kind=body,in=channelV2,scope=channel" choice:"1:DISPLAY" choice:"2:VIDEO" enumAccept:"scalar" enumInteger:"passthrough" enumCase:"upper" enumTrim:"space"`
	Has      *struct{ Request, Supplied, Channel bool } `json:"-" setMarker:"true"`
}

func TestNativePresenceThenScopedEnum(t *testing.T) {
	compiled, err := New(Input{Component: &spec.Component{Routes: []*spec.Route{{Method: "POST", Path: "/staged"}}}, InputType: reflect.TypeFor[stagedNativeInput]()}).Compile()
	require.NoError(t, err)
	route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/staged"})
	require.True(t, ok)
	require.NoError(t, route.PrepareScopePlans())
	early, _ := route.ScopePlan("")
	late, _ := route.ScopePlan("channel")
	for _, raw := range []string{`"bad"`, `1e1000`, `{"nested":1e1000}`, `[1e1000]`, `null`, `" DISPLAY "`} {
		p, err := body.New([]byte(`{"id":7,"channelV2":`+raw+`}`), "application/json", nil)
		require.NoError(t, err)
		injector, err := bindly.NewInjector()
		require.NoError(t, err)
		scope, err := injector.ForScope(p)
		require.NoError(t, err)
		var got stagedNativeInput
		require.NoError(t, scope.Bind(t.Context(), &got, bindly.WithPlan(early), bindly.WithSource(&got)), raw)
		require.Equal(t, 7, got.Request.ID)
		require.True(t, got.Supplied)
		require.True(t, got.Has.Supplied)
		require.False(t, got.Has.Channel)
		err = scope.Bind(t.Context(), &got, bindly.WithPlan(late), bindly.WithSource(&got))
		if raw == `null` || raw == `" DISPLAY "` {
			require.NoError(t, err)
			require.True(t, got.Has.Channel)
		} else {
			require.Error(t, err)
		}
	}
}
