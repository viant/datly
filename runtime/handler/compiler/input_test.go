package compiler

import (
	"context"
	"io/fs"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/viant/bindly"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	xcodec "github.com/viant/xdatly/codec"
)

type namedTestCodec struct {
	name string
}

func bindingByPath(t *testing.T, bindings []bindly.BindingSpec, path string) bindly.BindingSpec {
	t.Helper()
	for _, binding := range bindings {
		if binding.Path == path {
			return binding
		}
	}
	t.Fatalf("binding path %s was not found in %+v", path, bindings)
	return bindly.BindingSpec{}
}

func (c namedTestCodec) Value(_ context.Context, raw interface{}, _ ...xcodec.Option) (interface{}, error) {
	return raw, nil
}

type namedTestCodecFactory struct {
	instances map[string]xcodec.Instance
}

type resourceCodecFactory struct {
	resources fs.FS
}

type configCodecFactory struct {
	config *xcodec.Config
}

type transportCodec struct {
	calls int
	raw   any
}

func (c *transportCodec) Value(_ context.Context, raw interface{}, _ ...xcodec.Option) (interface{}, error) {
	c.calls++
	c.raw = raw
	value, err := strconv.Atoi(raw.(string))
	if err != nil {
		return nil, err
	}
	return []int{value}, nil
}

type transportCodecFactory struct {
	config *xcodec.Config
	codec  *transportCodec
}

func (f *transportCodecFactory) New(config *xcodec.Config, _ ...xcodec.Option) (xcodec.Instance, error) {
	f.config = config
	return f.codec, nil
}

func (f *configCodecFactory) New(config *xcodec.Config, _ ...xcodec.Option) (xcodec.Instance, error) {
	f.config = config
	return namedTestCodec{name: "config"}, nil
}

func (f *resourceCodecFactory) New(_ *xcodec.Config, options ...xcodec.Option) (xcodec.Instance, error) {
	resolved := xcodec.NewOptions(options)
	f.resources = resolved.ResourceFS
	return namedTestCodec{name: "resource"}, nil
}

func TestBuildParamCodecsPassesPackageResourceFS(t *testing.T) {
	type input struct {
		IDs []int
	}
	component := &spec.Component{Parameters: []*spec.Parameter{{
		Name:     "IDs",
		TypeExpr: "[]int",
		Codec:    &spec.Codec{Body: "structql"},
	}}}
	resourceMap := fstest.MapFS{"sql/ids.sql": {Data: []byte("SELECT id FROM `/`")}}
	resources := resource.New()
	if err := resources.Register("", resourceMap); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	factory := &resourceCodecFactory{}
	if _, err := (ParamCodecCompiler{Component: component, InputType: reflect.TypeOf(input{}), Factory: factory, Resources: resources}).Build(); err != nil {
		t.Fatalf("BuildParamCodecs() error = %v", err)
	}
	if factory.resources != fs.FS(resources) {
		t.Fatal("codec factory did not receive the shared Bindly resource store")
	}
}

func TestTransportCodecRequestsAuthoredSourceTypeAndRunsOnce(t *testing.T) {
	type input struct{ IDs []int }
	component := &spec.Component{Parameters: []*spec.Parameter{{
		Name: "IDs", Source: spec.BindSource{Kind: "query", Name: "ids"},
		TypeExpr: "string", OutputTypeExpr: "[]int", Codec: &spec.Codec{Body: "AsInts"},
	}}}
	factory := &transportCodecFactory{codec: &transportCodec{}}
	component.Routes = []*spec.Route{{Method: "GET", Path: "/items"}}
	compiled, err := New(Input{Component: component, InputType: reflect.TypeOf(input{}), CodecFactory: factory}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	if factory.config == nil || factory.config.SourceType != reflect.TypeOf("") || factory.config.DestinationType != reflect.TypeOf([]int{}) {
		t.Fatalf("codec config = %+v", factory.config)
	}
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	scope := testharness.Request{}.WithQuery(url.Values{"ids": {"7"}})
	scoped, err := injector.ForScope(scope.Providers()...)
	if err != nil {
		t.Fatal(err)
	}
	actual := &input{}
	route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/items"})
	if !ok {
		t.Fatal("compiled route input contract was not found")
	}
	if err = scoped.Bind(context.Background(), actual, bindly.WithPlan(route.Plan())); err != nil {
		t.Fatal(err)
	}
	if len(actual.IDs) != 1 || actual.IDs[0] != 7 || factory.codec.calls != 1 || factory.codec.raw != "7" {
		t.Fatalf("binding result = %+v, codec calls=%d raw=%#v", actual, factory.codec.calls, factory.codec.raw)
	}
}

func TestBuildParamCodecsUsesParamSourceAndDestinationTypes(t *testing.T) {
	type event struct{ ID int }
	type helper struct{ Values []int }
	type input struct {
		Events []*event
		IDs    helper
	}
	component := &spec.Component{Parameters: []*spec.Parameter{
		{Name: "Events", Source: spec.BindSource{Kind: "body", Name: "Data"}},
		{Name: "IDs", Source: spec.BindSource{Kind: "param", Name: "Events"}, Codec: &spec.Codec{Body: "structql", Args: []string{"SELECT ARRAY_AGG(ID) AS Values FROM `/` LIMIT 1"}}},
	}}
	factory := &configCodecFactory{}
	codecs, err := (ParamCodecCompiler{Component: component, InputType: reflect.TypeOf(input{}), Factory: factory}).Build()
	if err != nil {
		t.Fatal(err)
	}
	if factory.config == nil || factory.config.SourceType != reflect.TypeOf([]*event{}) ||
		factory.config.DestinationType != reflect.TypeOf(helper{}) {
		t.Fatalf("codec config = %+v", factory.config)
	}
	bindings, err := BuildBindingSpecs(component, reflect.TypeOf(input{}), codecs)
	if err != nil {
		t.Fatal(err)
	}
	if actual := bindingByPath(t, bindings, "IDs").SourceType; actual != reflect.TypeOf([]*event{}) {
		t.Fatalf("binding source type = %v, want %v", actual, reflect.TypeOf([]*event{}))
	}
}

func (f namedTestCodecFactory) New(cfg *xcodec.Config, _ ...xcodec.Option) (xcodec.Instance, error) {
	if cfg == nil {
		return nil, nil
	}
	if instance, ok := f.instances[cfg.Body]; ok {
		return instance, nil
	}
	return nil, nil
}

func fieldParamsForTest(t *testing.T, component *spec.Component, inputType reflect.Type) map[string]*spec.Parameter {
	t.Helper()
	fields, err := newContractFields(inputType)
	if err != nil {
		t.Fatal(err)
	}
	params, err := fields.fieldParams(component)
	if err != nil {
		t.Fatal(err)
	}
	return params
}

func TestBuildFieldParams_PrefersDefineOverShadowedSetDeclaration(t *testing.T) {
	component := &spec.Component{
		Name: "PreferDefineFieldParams",
		Parameters: []*spec.Parameter{
			{
				Name:        "User",
				Declaration: spec.DeclarationKindDefine,
				Source:      spec.BindSource{Kind: "body"},
				TypeExpr:    "PreferredUser",
			},
			{
				Name:        "User",
				Declaration: spec.DeclarationKindSet,
				Source:      spec.BindSource{Kind: "body"},
				TypeExpr:    "LegacyUser",
			},
			{
				Name:        "Audit",
				Declaration: spec.DeclarationKindSet,
				Source:      spec.BindSource{Kind: "query", Name: "audit"},
				TypeExpr:    "string",
			},
		},
	}
	inputType := reflect.TypeOf(struct {
		User  string
		Audit string
	}{})

	params := fieldParamsForTest(t, component, inputType)

	userParam := params["User"]
	if userParam == nil {
		t.Fatalf("expected User param mapping, got %+v", params)
	}
	if userParam.Declaration != spec.DeclarationKindDefine || userParam.TypeExpr != "PreferredUser" {
		t.Fatalf("expected define-backed User param, got declaration=%q type=%q", userParam.Declaration, userParam.TypeExpr)
	}

	auditParam := params["Audit"]
	if auditParam == nil {
		t.Fatalf("expected unmatched set param Audit to remain, got %+v", params)
	}
	if auditParam.Declaration != spec.DeclarationKindSet {
		t.Fatalf("expected Audit to keep set declaration, got %q", auditParam.Declaration)
	}
}

func TestBuildFieldParams_KeepsSetOnlyMappingUnchanged(t *testing.T) {
	component := &spec.Component{
		Name: "SetOnlyFieldParams",
		Parameters: []*spec.Parameter{
			{
				Name:        "User",
				Declaration: spec.DeclarationKindSet,
				Source:      spec.BindSource{Kind: "body"},
				TypeExpr:    "LegacyUser",
			},
		},
	}
	inputType := reflect.TypeOf(struct {
		User string
	}{})

	params := fieldParamsForTest(t, component, inputType)
	userParam := params["User"]
	if userParam == nil {
		t.Fatalf("expected set-only User param mapping, got %+v", params)
	}
	if userParam.Declaration != spec.DeclarationKindSet || userParam.TypeExpr != "LegacyUser" {
		t.Fatalf("expected unchanged set-only User param, got declaration=%q type=%q", userParam.Declaration, userParam.TypeExpr)
	}
}

func TestBuildParamCodecs_PrefersDefineOverShadowedSetDeclaration(t *testing.T) {
	component := &spec.Component{
		Name: "PreferDefineCodecParams",
		Parameters: []*spec.Parameter{
			{
				Name:        "Ids",
				Declaration: spec.DeclarationKindDefine,
				Source:      spec.BindSource{Kind: "query", Name: "ids"},
				TypeExpr:    "string",
				Codec:       &spec.Codec{Body: "DefineCodec"},
			},
			{
				Name:        "Ids",
				Declaration: spec.DeclarationKindSet,
				Source:      spec.BindSource{Kind: "query", Name: "ids"},
				TypeExpr:    "string",
				Codec:       &spec.Codec{Body: "SetCodec"},
			},
		},
	}
	inputType := reflect.TypeOf(struct {
		Ids string
	}{})
	factory := namedTestCodecFactory{instances: map[string]xcodec.Instance{
		"DefineCodec": namedTestCodec{name: "define"},
		"SetCodec":    namedTestCodec{name: "set"},
	}}

	codecs, err := (ParamCodecCompiler{Component: component, InputType: inputType, Factory: factory}).Build()
	if err != nil {
		t.Fatalf("unexpected BuildParamCodecs error: %v", err)
	}
	compiled, ok := codecs["Ids"]
	if !ok {
		t.Fatalf("expected codec for Ids, got %+v", codecs)
	}
	named, ok := compiled.Instance.(namedTestCodec)
	if !ok {
		t.Fatalf("unexpected codec instance %T", compiled.Instance)
	}
	if named.name != "define" {
		t.Fatalf("expected define-backed codec to win, got %q", named.name)
	}
}

func TestBuildFieldParams_ResolvesLowercaseParamToExportedField(t *testing.T) {
	component := &spec.Component{
		Parameters: []*spec.Parameter{
			{Name: "user", Source: spec.BindSource{Kind: "body"}},
		},
	}
	inputType := reflect.TypeOf(struct {
		User string
	}{})

	params := fieldParamsForTest(t, component, inputType)
	if params["User"] == nil {
		t.Fatalf("expected exported field key User, got %+v", params)
	}
	if _, exists := params["user"]; exists {
		t.Fatalf("did not expect lowercase field key, got %+v", params)
	}
}

func TestBuildBindingSpecsResolvesBindLogicalNameToPhysicalField(t *testing.T) {
	type input struct {
		Projection []string `bind:"Fields,kind=query,in=package_fields"`
	}
	component := &spec.Component{Parameters: []*spec.Parameter{{
		Name: "Fields", Source: spec.BindSource{Kind: "query", Name: "canonical_fields"},
	}}}
	bindings, err := BuildBindingSpecs(component, reflect.TypeOf(input{}), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(bindings) != 1 || bindings[0].Path != "Projection" || bindings[0].Name != "Fields" ||
		bindings[0].Location.Kind != "query" || bindings[0].Location.In != "canonical_fields" {
		t.Fatalf("bindings = %+v", bindings)
	}
}

func TestBuildBindingSpecsRejectsAmbiguousLogicalAlias(t *testing.T) {
	type input struct {
		First  string `bind:"Value,kind=query,in=first"`
		Second string `bind:"Value,kind=query,in=second"`
	}
	component := &spec.Component{Parameters: []*spec.Parameter{{Name: "Value", Source: spec.BindSource{Kind: "query"}}}}
	if _, err := BuildBindingSpecs(component, reflect.TypeOf(input{}), nil); err == nil || !strings.Contains(err.Error(), "matches multiple input fields") {
		t.Fatalf("BuildBindingSpecs() error = %v", err)
	}
}

func TestBuildBindingSpecs_UsesResolvedParamSource(t *testing.T) {
	component := &spec.Component{
		Parameters: []*spec.Parameter{
			{Name: "userId", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Audit", Source: spec.BindSource{Kind: "query", Name: "audit"}},
			{Name: "Payload", Source: spec.BindSource{Kind: "body"}},
		},
	}
	inputType := reflect.TypeOf(struct {
		UserId  int
		Audit   string
		Payload string
	}{})

	bindings, err := BuildBindingSpecs(component, inputType, nil)
	if err != nil {
		t.Fatalf("unexpected BuildBindingSpecs error: %v", err)
	}
	if got := bindingByPath(t, bindings, "UserId").Location.Kind; got != "path" {
		t.Fatalf("expected UserId kind path, got %q", got)
	}
	if got := bindingByPath(t, bindings, "UserId").Location.In; got != "id" {
		t.Fatalf("expected UserId source id, got %q", got)
	}
	if got := bindingByPath(t, bindings, "Audit").Location.Kind; got != "query" {
		t.Fatalf("expected Audit kind query, got %q", got)
	}
	if got := bindingByPath(t, bindings, "Audit").Location.In; got != "audit" {
		t.Fatalf("expected Audit source audit, got %q", got)
	}
	if bindingByPath(t, bindings, "Payload").Location.Kind != "body" {
		t.Fatalf("expected Payload to be body-bound")
	}
}

func TestBuildBindingSpecs_ParamSourceWinsOverConflictingFieldTag(t *testing.T) {
	component := &spec.Component{
		Parameters: []*spec.Parameter{
			{Name: "UserId", Source: spec.BindSource{Kind: "path", Name: "id"}},
		},
	}
	inputType := reflect.TypeOf(struct {
		UserId int `bind:"kind=query,in=userTag"`
	}{})

	bindings, err := BuildBindingSpecs(component, inputType, nil)
	if err != nil {
		t.Fatalf("unexpected BuildBindingSpecs error: %v", err)
	}
	if got := bindingByPath(t, bindings, "UserId").Location.Kind; got != "path" {
		t.Fatalf("expected resolved param kind path to win over field tag, got %q", got)
	}
	if got := bindingByPath(t, bindings, "UserId").Location.In; got != "id" {
		t.Fatalf("expected resolved param source id to win over field tag, got %q", got)
	}
}

func TestBuildBindingSpecs_FailsOnMissingScalarSourceName(t *testing.T) {
	component := &spec.Component{
		Parameters: []*spec.Parameter{
			{Name: "UserID", Source: spec.BindSource{Kind: "query"}},
		},
	}
	inputType := reflect.TypeOf(struct {
		UserID int
	}{})

	_, err := BuildBindingSpecs(component, inputType, nil)
	if err == nil {
		t.Fatalf("expected missing source name error, got nil")
	}
}

func TestBuildBindingSpecs_BindTagCompletesMissingScalarSourceName(t *testing.T) {
	component := &spec.Component{
		Parameters: []*spec.Parameter{
			{Name: "UserID", Source: spec.BindSource{Kind: "query"}},
			{Name: "Token", Source: spec.BindSource{Kind: "header"}},
		},
	}
	inputType := reflect.TypeOf(struct {
		UserID int    `bind:"kind=query,in=uid"`
		Token  string `bind:"kind=query,in=token"`
	}{})

	_, err := BuildBindingSpecs(component, inputType, nil)
	if err == nil {
		t.Fatalf("expected missing source name error for Token: query tag must not complete a header param")
	}

	component.Parameters = component.Parameters[:1]
	inputType = reflect.TypeOf(struct {
		UserID int `bind:"kind=query,in=uid"`
	}{})
	bindings, err := BuildBindingSpecs(component, inputType, nil)
	if err != nil {
		t.Fatalf("unexpected BuildBindingSpecs error: %v", err)
	}
	if got := bindingByPath(t, bindings, "UserID").Location.In; got != "uid" {
		t.Fatalf("expected matching-kind tag to complete source name uid, got %q", got)
	}
	if bindingByPath(t, bindings, "UserID").Extension == nil {
		t.Fatalf("expected tag-completed binding to keep resolved param")
	}
}

func TestBuildBindingSpecs_FailsWhenParamHasNoInputField(t *testing.T) {
	component := &spec.Component{
		Parameters: []*spec.Parameter{
			{Name: "MissingField", Source: spec.BindSource{Kind: "query", Name: "missing"}},
		},
	}
	inputType := reflect.TypeOf(struct {
		Present string
	}{})

	_, err := BuildBindingSpecs(component, inputType, nil)
	if err == nil {
		t.Fatalf("expected missing-field error")
	}
}

func TestBuildBindingSpecs_UsesBindTagsWhenNoParamExists(t *testing.T) {
	component := &spec.Component{}
	inputType := reflect.TypeOf(struct {
		ID      int    `bind:"kind=path,in=id"`
		Auth    string `bind:"kind=header,in=Authorization"`
		Payload string `bind:"kind=body"`
	}{})

	bindings, err := BuildBindingSpecs(component, inputType, nil)
	if err != nil {
		t.Fatalf("unexpected BuildBindingSpecs error: %v", err)
	}
	if got := bindingByPath(t, bindings, "ID").Location.Kind; got != "path" {
		t.Fatalf("expected ID kind path, got %q", got)
	}
	if got := bindingByPath(t, bindings, "ID").Location.In; got != "id" {
		t.Fatalf("expected ID source id, got %q", got)
	}
	if got := bindingByPath(t, bindings, "Auth").Location.Kind; got != "header" {
		t.Fatalf("expected Auth kind header, got %q", got)
	}
	if bindingByPath(t, bindings, "Payload").Location.Kind != "body" {
		t.Fatalf("expected Payload to be body-bound")
	}
}

func TestBuildParamCodecs_ResolvesLowercaseParamToExportedField(t *testing.T) {
	component := &spec.Component{
		Parameters: []*spec.Parameter{
			{Name: "ids", Source: spec.BindSource{Kind: "query", Name: "ids"}, TypeExpr: "string", Codec: &spec.Codec{Body: "DefineCodec"}},
		},
	}
	inputType := reflect.TypeOf(struct {
		Ids string
	}{})
	factory := namedTestCodecFactory{instances: map[string]xcodec.Instance{
		"DefineCodec": namedTestCodec{name: "define"},
	}}

	codecs, err := (ParamCodecCompiler{Component: component, InputType: inputType, Factory: factory}).Build()
	if err != nil {
		t.Fatalf("unexpected BuildParamCodecs error: %v", err)
	}
	if _, ok := codecs["Ids"]; !ok {
		t.Fatalf("expected exported field key Ids, got %+v", codecs)
	}
}

func TestCompileBuildsRouteEffectiveInputContracts(t *testing.T) {
	type input struct {
		Shared string
		ItemID int
	}
	component := &spec.Component{
		Routes: []*spec.Route{
			{Method: "GET", Path: "/items"},
			{Method: "GET", Path: "/items/{id}"},
		},
		Parameters: []*spec.Parameter{
			{Name: "Shared", Source: spec.BindSource{Kind: "query", Name: "q"}},
			{Name: "ItemID", Source: spec.BindSource{Kind: "path", Name: "id"}, Activation: &spec.RouteActivation{URI: "/items/{id}"}},
		},
	}
	result, err := New(Input{Component: component, InputType: reflect.TypeOf(input{})}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	collection, ok := result.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/items"})
	if !ok || len(collection.Fields()) != 1 || collection.Fields()[0].Path() != "Shared" {
		t.Fatalf("collection contract = (%+v, %v)", collection, ok)
	}
	detail, ok := result.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/items/{id}"})
	if !ok || len(detail.Fields()) != 2 || detail.Fields()[1].Path() != "ItemID" {
		t.Fatalf("detail contract = (%+v, %v)", detail, ok)
	}
}

func TestCompilerBuildsCanonicalInputProjection(t *testing.T) {
	type input struct {
		AccountID string `bind:"kind=query,in=accountId"`
		Derived   int
	}
	component := &spec.Component{
		Routes: []*spec.Route{
			{Method: "GET", Path: "/accounts"},
			{Method: "POST", Path: "/accounts"},
		},
		Parameters: []*spec.Parameter{
			{Name: "Account", Source: spec.BindSource{Kind: "query", Name: "accountId"}},
			{Name: "Derived"},
		},
	}
	compiled, err := New(Input{Component: component, InputType: reflect.TypeOf(input{})}).Compile()
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	actual := &input{AccountID: "acct-7", Derived: 9}
	resolver := compiled.Input.Resolver(actual)
	for name, want := range map[string]any{"AccountID": "acct-7", "Account": "acct-7", "accountId": "acct-7", "Derived": 9} {
		value, ok, valueErr := resolver(name)
		if valueErr != nil || !ok || value != want {
			t.Fatalf("Projection.Value(%q) = %#v, %v, %v; want %#v", name, value, ok, valueErr, want)
		}
	}
	get, getOK := compiled.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/accounts"})
	post, postOK := compiled.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/accounts"})
	if !getOK || !postOK || get.Plan() != post.Plan() {
		t.Fatal("routes without activation should share the canonical Bindly plan")
	}
}

func TestCompileRejectsTypedInputWithoutRoute(t *testing.T) {
	type input struct{}
	_, err := New(Input{Component: &spec.Component{}, InputType: reflect.TypeOf(input{})}).Compile()
	if err == nil || !strings.Contains(err.Error(), "requires at least one route") {
		t.Fatalf("Compile() error = %v, want explicit route requirement", err)
	}
}

func TestCompileRejectsUnknownOrAmbiguousRouteActivation(t *testing.T) {
	type input struct{ ID int }
	for _, testCase := range []struct {
		name   string
		routes []*spec.Route
		uri    string
		want   string
	}{
		{name: "unknown", routes: []*spec.Route{{Method: "GET", Path: "/items"}}, uri: "/missing", want: "does not match"},
		{name: "ambiguous", routes: []*spec.Route{{Method: "GET", Path: "/items"}, {Method: "POST", Path: "/items"}}, uri: "/items", want: "ambiguous"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := New(Input{
				Component: &spec.Component{Routes: testCase.routes, Parameters: []*spec.Parameter{{
					Name: "ID", Source: spec.BindSource{Kind: "query", Name: "id"}, Activation: &spec.RouteActivation{URI: testCase.uri},
				}}},
				InputType: reflect.TypeOf(input{}),
			}).Compile()
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("Compile() error = %v", err)
			}
		})
	}
}
