package tool

import (
	"reflect"
	"testing"

	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
)

type privateAnonymousFields struct {
	Hidden string `json:"hidden"`
}
type publicAnonymousBody struct {
	privateAnonymousFields `internal:"true"`
	Public                 bool   `json:"public" internal:"false"`
	Secret                 string `json:"secret" internal:"true" mcp:"name=public"`
}
type publicAnonymousInput struct {
	Payload publicAnonymousBody `anonymous:"true"`
}

func TestAnonymousSchemaCannotPublishInternalOwnersOrRenamedSecrets(t *testing.T) {
	contract := testRouteContract(t, reflect.TypeFor[publicAnonymousInput](), []bindly.BindingSpec{{
		Path: "Payload", Location: bindstate.Location{Kind: "body"},
	}})
	plan, err := NewCompiler().Compile(Input{Contract: contract,
		Component: spec.Key{Kind: spec.KindComponent, Name: "PublicBody"},
		Exposure:  &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "public.body"},
	})
	if err != nil {
		t.Fatal(err)
	}
	properties := plan.Metadata().InputSchema.Properties
	if len(properties) != 1 || properties["public"] == nil || len(plan.Arguments()) != 1 {
		t.Fatalf("private fields entered public arguments/schema: %v", properties)
	}
}
