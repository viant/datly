package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/provider/body"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xhandler "github.com/viant/xdatly/handler"
)

type presenceStageBody struct {
	Account int  `json:"account"`
	Deal    bool `json:"deal"`
}
type presenceStageInput struct {
	Request  *presenceStageBody                         `parameter:"kind=body"`
	Supplied bool                                       `parameter:"kind=body,in=channelV2,presenceOnly=true" internal:"true"`
	Channel  int                                        `parameter:"kind=body,in=channelV2,scope=channel" choice:"1:DISPLAY" choice:"2:VIDEO" enumAccept:"scalar" enumInteger:"passthrough" enumCase:"upper" enumTrim:"space"`
	Has      *struct{ Request, Supplied, Channel bool } `setMarker:"true"`
}
type presenceStageHandler struct{}

func (presenceStageHandler) BindsInputByScope() bool { return true }
func (presenceStageHandler) Exec(ctx context.Context, s xhandler.Session, in *presenceStageInput, out *scopedNativeOutput) error {
	if in.Has.Channel {
		return errors.New("channel eagerly bound")
	}
	if in.Request.Account != 3779 {
		return errors.New("account denied")
	}
	if !in.Supplied {
		return errors.New("channel required")
	}
	if !in.Request.Deal {
		return errors.New("deal missing")
	}
	if err := s.Binder().(xhandler.ScopedBinder).BindScope(ctx, in, "channel"); err != nil {
		return err
	}
	if !in.Has.Channel {
		return errors.New("channel marker lost")
	}
	out.Value = "bound"
	return nil
}
func TestPresenceOnlyPreservesStagedHandlerErrorOrder(t *testing.T) {
	parent := componentArtifact(t, componentSpec("PresenceStage", "POST", "/presence-stage", nil), reflect.TypeFor[presenceStageInput](), reflect.TypeFor[scopedNativeOutput]())
	parent.Handler = custom.New[presenceStageInput, scopedNativeOutput](presenceStageHandler{})
	registered, err := parent.Registration(registry.RegisteredComponent{})
	require.NoError(t, err)
	rt, err := NewRuntime([]*registry.RegisteredComponent{registered})
	require.NoError(t, err)
	target := dexec.ComponentTarget{Component: parent.Component.Key, Route: spec.RouteRef{Method: "POST", Path: "/presence-stage"}}
	for _, tc := range []struct{ raw, want string }{
		{`{"account":0,"channelV2":1e1000}`, "account denied"},
		{`{"account":3779}`, "channel required"},
		{`{"account":3779,"channelV2":{"x":1e1000}}`, "deal missing"},
		{`{"account":3779,"deal":true,"channelV2":1e1000}`, "conversion"},
		{`{"account":3779,"deal":true,"channelV2":" DISPLAY "}`, ""},
		{`{"account":3779,"deal":true,"channelV2":null}`, ""},
		{`{"account":3779,"deal":true,"channelV2":1e1000,"channelV2":1}`, ""},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			p, err := body.New([]byte(tc.raw), "application/json", nil)
			require.NoError(t, err)
			result, err := rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: target, Providers: []locator.Provider{p}})
			if tc.want == "" {
				require.NoError(t, err)
				require.Equal(t, "bound", result.(*scopedNativeOutput).Value)
			} else if tc.want == "conversion" {
				require.Error(t, err)
				require.NotContains(t, err.Error(), "account denied")
				require.NotContains(t, err.Error(), "deal missing")
			} else {
				require.ErrorContains(t, err, tc.want)
			}
		})
	}
}
