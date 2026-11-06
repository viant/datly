package engine

import (
	"net/http/httptest"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/provider/request"
	"github.com/viant/bindly/provider/values"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
)

type contractOperationScope []locator.Provider

func (s contractOperationScope) Providers() []locator.Provider { return s }
func TestContractOperationUsesDeclaredRouteAndPreservesTransportMetadata(t *testing.T) {
	root, _ := bindly.NewInjector()
	for _, method := range []string{"POST", "PATCH"} {
		target := dexec.ComponentTarget{Route: spec.RouteRef{Method: method, Path: "/campaigns"}}
		for _, metadataOnly := range []bool{false, true} {
			scope := request.NewValues()
			if !metadataOnly {
				var err error
				scope, err = request.New(httptest.NewRequest("GET", "/openapi.json", nil))
				if err != nil {
					t.Fatal(err)
				}
			}
			binder, err := ContractScope(root, nil, scope, nil, nil, target)
			if err != nil {
				t.Fatal(err)
			}
			var state struct {
				Method    string `bind:"kind=contract,in=method,required"`
				Path      string `bind:"kind=contract,in=path,required"`
				Transport string `bind:"kind=http_request,in=method"`
			}
			err = binder.Bind(t.Context(), &state)
			scope.Close()
			if err != nil || state.Method != method || state.Path != "/campaigns" {
				t.Fatalf("%+v %v", state, err)
			}
			if !metadataOnly && state.Transport != "GET" {
				t.Fatal("transport metadata overwritten")
			}
		}
		spoof := values.New("contract", map[string]any{"method": "DELETE"})
		if _, err := ContractScope(root, nil, contractOperationScope{spoof}, nil, nil, target); err == nil {
			t.Fatal("protocol replaced trusted target operation")
		}
		if _, err := ContractScope(root, []locator.Provider{spoof}, nil, nil, nil, target); err == nil {
			t.Fatal("component replaced trusted target operation")
		}
	}
}
