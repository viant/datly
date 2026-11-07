package tool

import (
	"reflect"
	"testing"
	"time"

	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"github.com/xeipuuv/gojsonschema"
)

type declaredTimeBody struct {
	Stamp *time.Time `json:"stamp,omitempty" format:"timeLayout=2006-01-02T15:04:05Z07:00,timeInputLayouts=2006-01-02T15:04:05.999999999Z07:00|2006-01-02T15:04:05|2006-01-02 15:04:05|2006-01-02,timeEpoch=milliseconds,timeEmpty=preserve,timezone=UTC,nullable=true"`
}
type declaredTimeInput struct {
	Body *declaredTimeBody `parameter:"Body,kind=body"`
}

func TestDeclaredTimeMCPInput(t *testing.T) {
	binding := bindly.BindingSpec{Path: "Body", Name: "Body", Location: bindstate.Location{Kind: "body"}}
	contract := testRouteContract(t, reflect.TypeFor[declaredTimeInput](), []bindly.BindingSpec{binding})
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "Times"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "times"}, Contract: contract})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		value any
		valid bool
		want  string
	}{
		{"epoch", 0, true, "1970-01-01T00:00:00Z"}, {"date", "2026-11-07", true, "2026-11-07T00:00:00Z"}, {"empty", "", true, "0001-01-01T00:00:00Z"}, {"offset", "2026-11-07T08:00:00-08:00", true, "2026-11-07T16:00:00Z"}, {"null", nil, true, "nil"}, {"bad", "nonsense", false, ""}, {"number string", "123", false, ""}, {"bool", true, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := map[string]any{"Body": map[string]any{"stamp": tc.value}}
			result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(plan.Metadata().InputSchema), gojsonschema.NewGoLoader(args))
			if err != nil {
				t.Fatal(err)
			}
			if result.Valid() != tc.valid {
				t.Errorf("schema valid=%v want %v: %v", result.Valid(), tc.valid, result.Errors())
			}
			scope, err := plan.Scope(args)
			if err != nil {
				if tc.valid {
					t.Fatal(err)
				}
				return
			}
			defer scope.Close()
			body, _, err := scope.Body().Locate(nil).Value(t.Context(), reflect.TypeFor[*declaredTimeBody](), "")
			if (err == nil) != tc.valid {
				t.Fatalf("bind error=%v", err)
			}
			if err != nil {
				return
			}
			stamp := body.(*declaredTimeBody).Stamp
			got := "nil"
			if stamp != nil {
				got = stamp.Format(time.RFC3339Nano)
			}
			if got != tc.want {
				t.Fatalf("got %s want %s", got, tc.want)
			}
		})
	}
}
