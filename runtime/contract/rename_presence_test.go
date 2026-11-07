package contract_test

import (
	"context"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	dcontract "github.com/viant/datly/runtime/contract"
	"github.com/viant/datly/spec"
	xcontract "github.com/viant/xdatly/contract"
)

type renamedState struct {
	Body *renamedBody `parameter:",kind=body,required=true"`
}
type renamedBody struct {
	Has   *renamedHas `setMarker:"true" json:"-"`
	Value int
	Other bool
}
type renamedHas struct{ Value, Other bool }

func (*renamedBody) Contract(context.Context, bindly.Binder) (*xcontract.Policy, error) {
	return &xcontract.Policy{Name: "rename", Field: func(_ string, parent reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		if parent == reflect.TypeFor[renamedBody]() && f.Name == "Value" {
			copy := f.Clone()
			copy.Name = "Renamed"
			return copy, nil
		}
		return nil, nil
	}}, nil
}

func TestRenamedFieldKeepsCanonicalPresenceWithMarkerDeclaredFirst(t *testing.T) {
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: &spec.Component{Routes: []*spec.Route{{Method: "POST", Path: "/renamed"}}}, InputType: reflect.TypeFor[renamedState](), OutputType: reflect.TypeFor[struct{}]()})
	if err != nil {
		t.Fatal(err)
	}
	root, _ := bindly.NewInjector()
	policy, err := dcontract.New(reflect.TypeFor[renamedState](), 8)
	if err != nil {
		t.Fatal(err)
	}
	effective, err := policy.Resolve(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	canonical, _ := artifact.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/renamed"})
	plan, err := canonical.WithEffectiveInput(effective)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		raw     string
		present bool
	}{{`{"Renamed":0,"Other":false}`, true}, {`{"Other":false}`, false}} {
		req := httptest.NewRequest("POST", "/renamed", strings.NewReader(tc.raw))
		req.Header.Set("Content-Type", "application/json")
		source, err := request.New(req)
		if err != nil {
			t.Fatal(err)
		}
		scope, err := root.ForScope(source.Providers()...)
		if err != nil {
			t.Fatal(err)
		}
		var out renamedState
		err = scope.Bind(t.Context(), &out, bindly.WithPlan(plan.Plan()))
		source.Close()
		if err != nil {
			t.Fatal(err)
		}
		if out.Body == nil || out.Body.Has == nil || out.Body.Has.Value != tc.present || !out.Body.Has.Other || out.Body.Value != 0 {
			t.Fatalf("presence lost for %s: %+v", tc.raw, out.Body)
		}
	}
}
