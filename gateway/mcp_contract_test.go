package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/repository/content"
	"github.com/viant/jsonrpc"
	"gopkg.in/yaml.v3"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/datly/repository"
	rcontract "github.com/viant/datly/repository/contract"
	dpath "github.com/viant/datly/repository/path"
	"github.com/viant/datly/repository/version"
	dynamic "github.com/viant/datly/service/contract"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/mcp-protocol/schema"
	serverproto "github.com/viant/mcp-protocol/server"
	xcontract "github.com/viant/xdatly/contract"
)

type mcpContractRow struct {
	Secret string `json:"secret"`
	Mode   int    `json:"mode" choice:"0:draft" choice:"10:active"`
}

func (*mcpContractRow) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	var op struct {
		Method string `parameter:",kind=contract,in=method"`
		Debug  bool   `bind:",kind=header,in=X-Debug"`
	}
	if err := binder.Bind(ctx, &op); err != nil {
		return nil, err
	}
	return &xcontract.Policy{Name: fmt.Sprintf("same-name:%t", op.Debug), Field: func(_ string, parent reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		if parent != reflect.TypeFor[mcpContractRow]() {
			return nil, nil
		}
		switch f.Name {
		case "Secret":
			return f.Hide(), nil
		case "Mode":
			if op.Debug {
				f = f.SetDescription("debug")
			}
			if op.Method == "POST" {
				return f.SetTag("enumAccept", "literal").SetTag("enumStyle", "ordinal").Required(), nil
			}
			return f.SetTag("enumAccept", "ordinal").SetTag("enumStyle", "literal").Optional(), nil
		}
		return nil, nil
	}}, nil
}

type mcpContractInput struct {
	Body []*mcpContractRow `parameter:",kind=body"`
}

func contractTestComponent(t *testing.T) *repository.Component {
	t.Helper()
	input, err := state.NewType(state.WithSchema(state.NewSchema(reflect.TypeFor[mcpContractInput]())))
	if err != nil {
		t.Fatal(err)
	}
	c := &repository.Component{Path: rcontract.Path{Method: "POST", URI: "/campaigns"}, View: &view.View{Name: "campaigns"}}
	c.View.SetResource(&view.Resource{})
	c.Input.Type = *input
	c.Input.Body.Schema = state.NewSchema(reflect.TypeFor[[]*mcpContractRow]())
	c.Output.Type.Schema = state.NewSchema(reflect.TypeFor[*mcpContractInput]())
	return c
}
func TestContractMCPMethodVariants(t *testing.T) {
	c := contractTestComponent(t)
	router := &Router{}
	for _, method := range []string{"POST", "PATCH", "POST"} {
		tool, err := router.mcpMetadata(context.Background(), c, &dpath.Path{Path: rcontract.Path{Method: method, URI: c.URI}}, schema.Tool{Name: method})
		if err != nil {
			t.Fatal(err)
		}
		// Anonymous slice bodies are advertised under their body parameter.
		body := tool.InputSchema.Properties["Body"]
		if body == nil {
			t.Fatalf("body schema missing: %+v", tool.InputSchema)
		}
		row := body["items"].(map[string]any)
		properties := row["properties"].(map[string]any)
		if properties["secret"] != nil {
			t.Fatal("hidden field exposed")
		}
		mode := properties["mode"].(map[string]any)
		want := "string"
		if method == "PATCH" {
			want = "integer"
		}
		if mode["type"] != want {
			t.Fatalf("%s mode: %+v", method, mode)
		}
		outputMode := tool.OutputSchema.Properties["Body"]["items"].(map[string]any)["properties"].(map[string]any)["mode"].(map[string]any)
		outputWant := "integer"
		if method == "PATCH" {
			outputWant = "string"
		}
		if outputMode["type"] != outputWant {
			t.Fatalf("%s output mode: %+v", method, outputMode)
		}

		if method == "POST" && len(row["required"].([]any)) != 1 {
			t.Fatalf("required: %+v", row)
		}
	}
}

func TestContractMCPInvocationPreservesEnumWire(t *testing.T) {
	c := contractTestComponent(t)
	c.Content.Marshaller.JSON.RuntimeMarshaller = &content.StructologyJSONRuntime{}
	c.Content.Marshaller.JSON.RuntimeUnmarshaller = &content.StructologyJSONRuntime{}
	router := &Router{}
	for _, method := range []string{"POST", "PATCH"} {
		calls := 0
		route := &Route{Path: &rcontract.Path{Method: method, URI: c.URI}, Handler: func(ctx context.Context, w http.ResponseWriter, req *http.Request) {
			calls++
			effective, err := router.mcpContract(ctx, c, method, c.URI)
			if err != nil {
				t.Error(err)
				http.Error(w, err.Error(), 500)
				return
			}
			raw, err := io.ReadAll(req.Body)
			if err != nil {
				t.Error(err)
				return
			}
			var rows []*mcpContractRow
			if err = c.UnmarshalFor(repository.WithUnmarshalRequest(req), repository.WithUnmarshalRepresentation(effective.Body))(raw, &rows); err != nil {
				t.Errorf("%s decoding %s: %v", method, raw, err)
				http.Error(w, err.Error(), 500)
				return
			}
			if len(rows) != 1 || rows[0].Mode != 10 || rows[0].Secret != "" {
				t.Errorf("decoded: %+v", rows)
			}
			result, err := c.MarshalFunc(repository.WithRepresentation(effective.Output))(mcpContractInput{Body: rows})
			if err != nil {
				t.Error(err)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(result)
		}}
		var mode any = "active"
		if method == "PATCH" {
			mode = 10.0
		}
		result, rpcErr := router.mcpToolCallHandler(c, route)(context.Background(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Arguments: map[string]any{"Body": []any{map[string]any{"mode": mode, "secret": "blocked"}}}}})
		if rpcErr != nil || result == nil || calls != 1 {
			t.Fatalf("%s calls=%d result=%+v err=%v", method, calls, result, rpcErr)
		}
	}
}

func TestContractViewMetadataDoesNotMutateView(t *testing.T) {
	c := contractTestComponent(t)
	c.View.Schema = state.NewSchema(reflect.TypeFor[mcpContractRow]())
	c.View.Columns = []*view.Column{{Name: "Secret", Tag: `json:"secret"`}, {Name: "Mode", Tag: `json:"mode"`}}
	effective, err := (&Router{}).mcpContract(context.Background(), c, "POST", c.URI)
	if err != nil {
		t.Fatal(err)
	}
	// This fixture's output view is the Body member.
	metadata, err := effective.Output.Lookup("Body")
	if err != nil {
		t.Fatal(err)
	}
	code, data := viewMetaResponse(c, metadata)
	if code != 200 {
		t.Fatalf("%d %s", code, data)
	}
	var doc map[string]any
	if err = yaml.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	columns := doc["Columns"].([]any)
	if len(columns) != 1 || columns[0].(map[string]any)["Name"] != "Mode" {
		t.Fatalf("%s", data)
	}
	if doc["EffectiveSchema"] == nil {
		t.Fatalf("effective schema missing: %s", data)
	}
	if len(c.View.Columns) != 2 || c.View.Columns[0].Tag != `json:"secret"` {
		t.Fatal("registered view mutated")
	}
}

type mcpRenamedBody struct {
	Value int `json:"value"`
}

func (*mcpRenamedBody) Contract(context.Context, bindly.Binder) (*xcontract.Policy, error) {
	return &xcontract.Policy{Name: "rename", Field: func(_ string, parent reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		if parent == reflect.TypeFor[mcpRenamedBody]() && f.Name == "Value" {
			return f.SetTag("json", "amount").SetTag("mcp", "aliases=amountAlias").SetMCPName("Amount"), nil
		}
		return nil, nil
	}}, nil
}

type mcpFlatInput struct {
	Body *mcpRenamedBody `parameter:",kind=body"`
}

func TestContractMCPFlattenedRenameReachesHTTPDecoder(t *testing.T) {
	c := contractTestComponent(t)
	input, err := state.NewType(state.WithSchema(state.NewSchema(reflect.TypeFor[mcpFlatInput]())))
	if err != nil {
		t.Fatal(err)
	}
	c.Input.Type = *input
	c.Input.Body.Schema = state.NewSchema(reflect.TypeFor[*mcpRenamedBody]())
	c.Content.Marshaller.JSON.RuntimeUnmarshaller = &content.StructologyJSONRuntime{}
	router := &Router{}
	var actual mcpRenamedBody
	route := &Route{Path: &c.Path, Handler: func(ctx context.Context, w http.ResponseWriter, req *http.Request) {
		effective, err := router.mcpContract(ctx, c, c.Method, c.URI)
		if err != nil {
			t.Error(err)
			return
		}
		data, err := io.ReadAll(req.Body)
		if err != nil {
			t.Error(err)
			return
		}
		if err = c.UnmarshalFor(repository.WithUnmarshalRepresentation(effective.Body))(data, &actual); err != nil {
			t.Errorf("%s: %v", data, err)
		}
		_, _ = w.Write([]byte(`{}`))
	}}
	_, rpcErr := router.mcpToolCallHandler(c, route)(context.Background(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Arguments: map[string]any{"amountAlias": 7.0}}})
	if rpcErr != nil || actual.Value != 7 {
		t.Fatalf("value=%d err=%v", actual.Value, rpcErr)
	}
}

func TestMCPNamedBodiesMergeAndPreserveExactOrdinal(t *testing.T) {
	router := &Router{}
	var body io.Reader
	for _, p := range []*state.Parameter{
		state.NewParameter("First", state.NewBodyLocation("first"), state.WithParameterSchema(state.NewSchema(reflect.TypeFor[int64]()))),
		state.NewParameter("Second", state.NewBodyLocation("second"), state.WithParameterSchema(state.NewSchema(reflect.TypeFor[int64]()))),
	} {
		var rpcErr *jsonrpc.Error
		_, body, rpcErr = router.applyParamToRequest("/", nil, p, json.Number("9007199254740993"), map[string]bool{}, map[string]bool{}, body)
		if rpcErr != nil {
			t.Fatal(rpcErr)
		}
	}
	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	var actual map[string]json.Number
	if err = json.Unmarshal(data, &actual); err != nil {
		t.Fatal(err)
	}
	if actual["first"] != "9007199254740993" || actual["second"] != "9007199254740993" {
		t.Fatalf("%s", data)
	}
}

func TestContractMCPBindsTransportHeaderState(t *testing.T) {
	c := contractTestComponent(t)
	router := &Router{}
	target := &dpath.Path{Path: c.Path}
	ctx := WithMCPHeaders(context.Background(), http.Header{"X-Debug": []string{"true"}})
	debug, err := router.mcpMetadata(ctx, c, target, schema.Tool{Name: "debug"})
	if err != nil {
		t.Fatal(err)
	}
	mode := debug.InputSchema.Properties["Body"]["items"].(map[string]any)["properties"].(map[string]any)["mode"].(map[string]any)
	if mode["description"] != "debug" {
		t.Fatalf("header not bound: %+v", mode)
	}
	normal, err := router.mcpMetadata(context.Background(), c, target, schema.Tool{Name: "normal"})
	if err != nil {
		t.Fatal(err)
	}
	mode = normal.InputSchema.Properties["Body"]["items"].(map[string]any)["properties"].(map[string]any)["mode"].(map[string]any)
	if mode["description"] != nil {
		t.Fatalf("header policy leaked: %+v", mode)
	}
}

type MCPContractEmbeddedBase struct {
	Label  string `json:"label"`
	Secret string `json:"secret"`
}

func (*MCPContractEmbeddedBase) Linked() {}

type MCPContractEmbeddedMiddle struct {
	*MCPContractEmbeddedBase
	Note string `json:"note"`
}
type mcpEmbeddedComposite struct {
	Before int `json:"before"`
	*MCPContractEmbeddedMiddle
	Rows  map[string][][]*mcpContractRow `json:"rows"`
	After bool                           `json:"after"`
}

func (*mcpEmbeddedComposite) Contract(context.Context, bindly.Binder) (*xcontract.Policy, error) {
	return &xcontract.Policy{Name: "embedded", Field: func(_ string, parent reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		if parent == reflect.TypeFor[MCPContractEmbeddedBase]() && f.Name == "Secret" {
			return f.Hide(), nil
		}
		return nil, nil
	}}, nil
}

type mcpEmbeddedInput struct {
	Body *mcpEmbeddedComposite `parameter:",kind=body"`
}

func TestContractMCPMultiLevelMethodEmbedding(t *testing.T) {
	c := contractTestComponent(t)
	input, err := state.NewType(state.WithSchema(state.NewSchema(reflect.TypeFor[mcpEmbeddedInput]())))
	if err != nil {
		t.Fatal(err)
	}
	c.Input.Type = *input
	c.Input.Body.Schema = state.NewSchema(reflect.TypeFor[*mcpEmbeddedComposite]())
	registry := serverproto.NewRegistry()
	router := &Router{mcpRegistry: registry}
	target := &dpath.Path{Path: c.Path, ModelContextProtocol: rcontract.ModelContextProtocol{MCPTool: true}}
	provider := repository.NewProvider(c.Path, &version.Control{}, func(context.Context, ...repository.Option) (*repository.Component, error) { return c, nil })
	if err = router.buildToolsIntegration(&dpath.Item{Paths: []*dpath.Path{target}}, target, &Route{Path: &c.Path}, provider); err != nil {
		t.Fatal(err)
	}
	for _, tool := range registry.ListRegisteredTools() {
		if len(tool.InputSchema.Properties) != 0 {
			t.Fatal("unresolved registration exposed caller fields")
		}
	}
	tools, err := registry.ListToolsFor(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tools) != 1 {
		t.Fatalf("%+v", tools)
	}
	fields := tools[0].InputSchema.Properties
	for _, name := range []string{"before", "label", "note", "rows", "after"} {
		if fields[name] == nil {
			t.Fatalf("missing %s: %+v", name, fields)
		}
	}
	if fields["secret"] != nil {
		t.Fatal("embedded secret exposed")
	}
	args, adjusted, err := router.contractToolArguments(context.Background(), c, &Route{Path: &c.Path}, map[string]any{"label": "linked", "before": 1.0, "rows": map[string]any{"a": []any{[]any{map[string]any{"mode": "active"}}}}})
	if err != nil || !adjusted || args["label"] != "linked" {
		t.Fatalf("%+v %t %v", args, adjusted, err)
	}
}

type mcpScopedInput struct {
	ID   int             `parameter:",kind=query,in=id"`
	Body *mcpRenamedBody `parameter:",kind=body"`
}

func (*mcpScopedInput) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	var scope struct {
		ID int `parameter:",kind=query,in=id"`
	}
	if err := binder.Bind(ctx, &scope); err != nil {
		return nil, err
	}
	return &xcontract.Policy{Name: fmt.Sprintf("id:%d", scope.ID), Field: func(_ string, parent reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		if scope.ID == 7 && parent == reflect.TypeFor[mcpRenamedBody]() && f.Name == "Value" {
			return f.Hide(), nil
		}
		return nil, nil
	}}, nil
}
func TestContractMCPInvocationBindsCanonicalQuery(t *testing.T) {
	c := contractTestComponent(t)
	input, err := state.NewType(state.WithSchema(state.NewSchema(reflect.TypeFor[mcpScopedInput]())))
	if err != nil {
		t.Fatal(err)
	}
	c.Input.Type = *input
	c.Input.Body.Schema = state.NewSchema(reflect.TypeFor[*mcpRenamedBody]())
	effective, err := (&Router{}).mcpInvocationContract(context.Background(), c, &Route{Path: &c.Path}, map[string]any{"ID": 7.0, "Amount": 3.0})
	if err != nil {
		t.Fatal(err)
	}
	metadata := effective.Body
	for metadata.Original().Kind() == reflect.Pointer {
		metadata = metadata.Elem()
	}
	if metadata.Field(0).Tag.Get("json") != "-" {
		t.Fatal("invocation query was not bound")
	}
}

func TestContractMCPRegistrationUsesCurrentGeneration(t *testing.T) {
	current := contractTestComponent(t)
	control := &version.Control{}
	provider := repository.NewProvider(current.Path, control, func(context.Context, ...repository.Option) (*repository.Component, error) { return current, nil })
	registry := serverproto.NewRegistry()
	router := &Router{mcpRegistry: registry}
	target := &dpath.Path{Path: current.Path, ModelContextProtocol: rcontract.ModelContextProtocol{MCPTool: true}}
	if err := router.buildToolsIntegration(&dpath.Item{Paths: []*dpath.Path{target}}, target, &Route{Path: &target.Path}, provider); err != nil {
		t.Fatal(err)
	}
	before, err := registry.ListToolsFor(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if before[0].InputSchema.Properties["Body"] == nil {
		t.Fatal("initial generation missing")
	}
	replacement := contractTestComponent(t)
	input, err := state.NewType(state.WithSchema(state.NewSchema(reflect.TypeFor[mcpFlatInput]())))
	if err != nil {
		t.Fatal(err)
	}
	replacement.Input.Type = *input
	replacement.Input.Body.Schema = state.NewSchema(reflect.TypeFor[*mcpRenamedBody]())
	current = replacement
	control.Increase()
	after, err := registry.ListToolsFor(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if after[0].InputSchema.Properties["Amount"] == nil || after[0].InputSchema.Properties["Body"] != nil {
		t.Fatalf("stale generation: %+v", after[0].InputSchema)
	}
	if before[0].InputSchema.Properties["Body"] == nil {
		t.Fatal("previous discovery mutated")
	}
}

func TestContractMCPEnumRejectsOppositeJSONTypeBeforeQueryEncoding(t *testing.T) {
	cases := []struct {
		field              reflect.StructField
		accepted, rejected any
	}{
		{reflect.StructField{Name: "Mode", Type: reflect.TypeFor[int](), Tag: `choice:"0:0" choice:"10:10" enumAccept:"literal"`}, "10", 10.0},
		{reflect.StructField{Name: "Mode", Type: reflect.TypeFor[string](), Tag: `choice:"0:draft" choice:"10:active" enumAccept:"ordinal"`}, 10.0, "10"},
	}
	for _, tc := range cases {
		if err := validateMCPEnum(tc.field, tc.accepted); err != nil {
			t.Fatal(err)
		}
		if err := validateMCPEnum(tc.field, tc.rejected); err == nil {
			t.Fatalf("accepted wrong JSON form %T", tc.rejected)
		}
	}
}

type mcpSelectorContractInput struct {
	ChildLimit int `parameter:",kind=query,in=match_limit"`
}

func (*mcpSelectorContractInput) Contract(context.Context, bindly.Binder) (*xcontract.Policy, error) {
	return &xcontract.Policy{Name: "hidden-selector", Field: func(_ string, parent reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		if parent == reflect.TypeFor[mcpSelectorContractInput]() && f.Name == "ChildLimit" {
			return f.Hide(), nil
		}
		return nil, nil
	}}, nil
}
func TestContractMCPSelectorUsesCanonicalFieldPolicy(t *testing.T) {
	input, err := state.NewType(state.WithSchema(state.NewSchema(reflect.TypeFor[mcpSelectorContractInput]())))
	if err != nil {
		t.Fatal(err)
	}
	parameter := input.Parameters.Lookup("ChildLimit")
	c := &repository.Component{View: &view.View{Selector: &view.Config{Constraints: &view.Constraints{Limit: true}}, With: []*view.Relation{{Holder: "Match", Of: &view.ReferenceView{View: view.View{Selector: &view.Config{LimitParameter: parameter}}}}}}}
	c.Input.Type = *input
	plan, err := dynamic.New(reflect.TypeFor[mcpSelectorContractInput](), 8)
	if err != nil {
		t.Fatal(err)
	}
	binder, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := plan.ResolveRepresentation(context.Background(), binder)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := (&Router{}).mcpInputSchema(c, nil, &repository.EffectiveContract{Input: metadata})
	if err != nil {
		t.Fatal(err)
	}
	properties := doc["properties"].(map[string]any)
	if properties["MatchLimit"] != nil || properties["ChildLimit"] != nil || properties["Limit"] == nil {
		t.Fatalf("%+v", properties)
	}
	if parameter.Name != "ChildLimit" {
		t.Fatal("canonical selector mutated")
	}
}

func TestContractMCPScalarBodyAndEmptyLiteralEncoding(t *testing.T) {
	router := &Router{}
	bodyParameter := state.NewParameter("Body", state.NewBodyLocation(""), state.WithParameterSchema(state.NewSchema(reflect.TypeFor[int]())))
	_, body, rpcErr := router.applyParamToRequest("/", nil, bodyParameter, "active", map[string]bool{}, map[string]bool{}, nil, mcpRequestEncoding{jsonBody: true})
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	raw, err := io.ReadAll(body)
	if err != nil || string(raw) != `"active"` {
		t.Fatalf("%s %v", raw, err)
	}
	query := url.Values{}
	parameter := state.NewParameter("Mode", state.NewQueryLocation("mode"), state.WithParameterSchema(state.NewSchema(reflect.TypeFor[int]())))
	_, _, rpcErr = router.applyParamToRequest("/", query, parameter, "", map[string]bool{}, map[string]bool{}, nil, mcpRequestEncoding{preserveEmpty: true})
	if rpcErr != nil || !query.Has("mode") {
		t.Fatalf("explicit empty literal lost: %v %v", query, rpcErr)
	}
}
