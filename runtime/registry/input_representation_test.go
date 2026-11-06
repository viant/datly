package registry_test

import (
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
	"github.com/viant/x/shape"
)

type InputMetadataEmbedded struct {
	Label string `json:"label"`
}

func (v *InputMetadataEmbedded) LinkedLabel() string { return v.Label }

type inputMetadataLeaf struct {
	*InputMetadataEmbedded
	Value  int                                  `json:"value"`
	Secret string                               `json:"secret"`
	Has    *struct{ Label, Value, Secret bool } `json:"-" setMarker:"true"`
}
type inputMetadataGroup struct {
	*InputMetadataEmbedded
	Rows [][]*inputMetadataLeaf       `json:"rows"`
	ByID map[int][]*inputMetadataLeaf `json:"byId"`
}
type inputMetadataBody struct {
	Groups []*inputMetadataGroup `json:"groups"`
}
type inputMetadataRoot struct {
	Body *inputMetadataBody `parameter:",kind=body,required=true"`
}

func TestInputRepresentationBindsMultiLevelLinkedEmbeddings(t *testing.T) {
	original := reflect.TypeFor[inputMetadataRoot]()
	component := &spec.Component{Routes: []*spec.Route{{Method: "POST", Path: "/metadata-input"}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: original, OutputType: reflect.TypeFor[struct{}]()})
	if err != nil {
		t.Fatal(err)
	}
	canonical, _ := artifact.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/metadata-input"})
	walker, _ := shape.CompileRewrite(original)
	metadata, err := walker.RewriteRepresentation(func(_ string, parent reflect.Type, f *shape.FieldMetadata) (*shape.FieldMetadata, error) {
		if parent == reflect.TypeFor[inputMetadataLeaf]() {
			switch f.Name {
			case "Secret":
				return &shape.FieldMetadata{Name: f.Name, Tag: `json:"-" internal:"true"`}, nil
			case "Value":
				return &shape.FieldMetadata{Name: "Renamed", Tag: `json:"amount" required:"true"`}, nil
			}
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = metadata.Reflected(); err == nil {
		t.Fatal("fixture must require metadata rather than a reflected replacement")
	}
	plan, err := canonical.WithInputRepresentation(metadata)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Type() != original {
		t.Fatal("handler input type replaced")
	}
	for _, tc := range []struct {
		raw   string
		valid bool
	}{
		{`{"groups":[{"label":"group","rows":[[{"label":"leaf","amount":0,"secret":"blocked"},null]],"byId":{"5":[{"amount":7,"secret":"blocked"}]}}]}`, true},
		{`{"groups":[{"rows":[[{"value":1}]]}]}`, false},
	} {
		req := httptest.NewRequest("POST", "/metadata-input", strings.NewReader(tc.raw))
		req.Header.Set("Content-Type", "application/json")
		source, err := request.New(req)
		if err != nil {
			t.Fatal(err)
		}
		injector, _ := bindly.NewInjector()
		scope, err := injector.ForScope(source.Providers()...)
		if err != nil {
			t.Fatal(err)
		}
		var actual inputMetadataRoot
		err = scope.Bind(t.Context(), &actual, bindly.WithPlan(plan.Plan()))
		source.Close()
		if !tc.valid {
			if err == nil {
				t.Fatal("missing effective required member accepted")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		group := actual.Body.Groups[0]
		leaf := group.Rows[0][0]
		mapped := group.ByID[5][0]
		if group.LinkedLabel() != "group" || leaf.LinkedLabel() != "leaf" || group.Rows[0][1] != nil {
			t.Fatal("embedded linked instances lost")
		}
		if leaf.Has == nil || !leaf.Has.Value || !leaf.Has.Label || leaf.Has.Secret || leaf.Secret != "" || mapped.Has == nil || !mapped.Has.Value || mapped.Value != 7 || mapped.Secret != "" {
			t.Fatal("nested admission/presence lost")
		}
	}
	fields, err := canonical.MetadataRepresentation(metadata, nil)
	if err != nil {
		t.Fatal(err)
	}
	if fields[0].Representation() == nil || fields[0].Representation().Original() != reflect.TypeFor[*inputMetadataBody]() {
		t.Fatal("publication lost the body metadata")
	}
	if canonical.Fields()[0].Representation() != nil {
		t.Fatal("canonical registration mutated")
	}
}
