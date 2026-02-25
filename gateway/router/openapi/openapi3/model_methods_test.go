package openapi3

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type yamlUnmarshaller interface {
	UnmarshalYAML(ctx context.Context, fn func(dest interface{}) error) error
}

func yamlDecoder(first interface{}, ext map[string]interface{}, firstErr, secondErr error) func(dest interface{}) error {
	call := 0
	return func(dest interface{}) error {
		call++
		if call == 1 {
			if firstErr != nil {
				return firstErr
			}
			if first == nil {
				return nil
			}
			b, err := json.Marshal(first)
			if err != nil {
				return err
			}
			return json.Unmarshal(b, dest)
		}
		if secondErr != nil {
			return secondErr
		}
		if ext == nil {
			ext = map[string]interface{}{}
		}
		b, err := json.Marshal(ext)
		if err != nil {
			return err
		}
		return json.Unmarshal(b, dest)
	}
}

func TestMergeJSON(t *testing.T) {
	tests := []struct {
		name   string
		j1     []byte
		j2     []byte
		expect string
	}{
		{name: "empty base", j1: []byte("{}"), j2: []byte(`{"x-a":1}`), expect: `{"x-a":1}`},
		{name: "merged", j1: []byte(`{"a":1}`), j2: []byte(`{"x-a":1}`), expect: `{"a":1,"x-a":1}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := string(mergeJSON(tt.j1, tt.j2)); got != tt.expect {
				t.Fatalf("expected %s, got %s", tt.expect, got)
			}
		})
	}
}

func TestExtensionFunctions(t *testing.T) {
	t.Run("unmarshal json keeps x keys", func(t *testing.T) {
		ext := Extension{}
		if err := ext.UnmarshalJSON([]byte(`{"x-a":1,"a":2}`)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := ext["x-a"]; !ok {
			t.Fatalf("expected x-a key")
		}
		if _, ok := ext["a"]; ok {
			t.Fatalf("did not expect non-extension key")
		}
	})

	t.Run("custom extension yaml", func(t *testing.T) {
		custom := CustomExtension{}
		fn := yamlDecoder(map[string]interface{}{"x-a": 1, "a": 2}, nil, nil, nil)
		if err := custom.UnmarshalYAML(context.Background(), fn); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := custom["x-a"]; !ok {
			t.Fatalf("expected x-a")
		}
		if _, ok := custom["a"]; ok {
			t.Fatalf("did not expect a")
		}
	})
}

func TestMarshalJSONWithExtensions_Table(t *testing.T) {
	trueValue := true
	tests := []struct {
		name    string
		value   interface{}
		wantErr bool
	}{
		{name: "components", value: &Components{Extension: Extension{"x-a": 1}, Schemas: Schemas{"Pet": {Type: "object"}}}},
		{name: "parameter", value: &Parameter{Extension: Extension{"x-a": 1}, Name: "id", In: "query"}},
		{name: "security", value: &SecurityScheme{Extension: Extension{"x-a": 1}, Type: "http"}},
		{name: "oauth flows", value: &OAuthFlows{Extension: Extension{"x-a": 1}, Password: &OAuthFlow{TokenURL: "token", Scopes: map[string]string{"s": "v"}}}},
		{name: "oauth flow", value: &OAuthFlow{Extension: Extension{"x-a": 1}, TokenURL: "token", Scopes: map[string]string{"s": "v"}}},
		{name: "example", value: &Example{Extension: Extension{"x-a": 1}, Summary: "s"}},
		{name: "server", value: &Server{Extension: Extension{"x-a": 1}, URL: "http://example"}},
		{name: "server variable", value: &ServerVariable{Extension: Extension{"x-a": 1}, Default: "dev"}},
		{name: "info", value: &Info{Extension: Extension{"x-a": 1}, Title: "api", Version: "1.0"}},
		{name: "contact", value: &Contact{Extension: Extension{"x-a": 1}, Name: "n"}},
		{name: "license", value: &License{Extension: Extension{"x-a": 1}, Name: "mit"}},
		{name: "tag", value: &Tag{Extension: Extension{"x-a": 1}, Name: "n"}},
		{name: "path item", value: &PathItem{Extension: Extension{"x-a": 1}, Summary: "sum"}},
		{name: "encoding", value: &Encoding{Extension: Extension{"x-a": 1}, ContentType: "application/json", Explode: &trueValue}},
		{name: "request body", value: &RequestBody{Extension: Extension{"x-a": 1}, Description: "d"}},
		{name: "external doc", value: &ExternalDocumentation{Extension: Extension{"x-a": 1}, URL: "http://example"}},
		{name: "response", value: &Response{Extension: Extension{"x-a": 1}, Description: strPtr("ok")}},
		{name: "media", value: &MediaType{Extension: Extension{"x-a": 1}, Example: map[string]interface{}{"a": 1}}},
		{name: "operation", value: &Operation{Extension: Extension{"x-a": 1}, Summary: "sum"}},
		{name: "link", value: &Link{Extension: Extension{"x-a": 1}, OperationID: "op"}},
		{name: "schema", value: &Schema{Extension: Extension{"x-a": 1}, Type: "object"}},
		{name: "xml", value: &XML{Extension: Extension{"x-a": 1}, Name: "node"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.value)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected marshal error")
				}
				return
			}
			if err != nil {
				t.Fatalf("marshal error: %v", err)
			}
			if !strings.Contains(string(data), "x-a") {
				t.Fatalf("expected extension in json: %s", string(data))
			}
		})
	}
}

func TestUnmarshalJSON_Table(t *testing.T) {
	tests := []struct {
		name   string
		target interface{}
		json   string
	}{
		{name: "components", target: &Components{}, json: `{"schemas":{"Pet":{"type":"object"}}}`},
		{name: "parameter", target: &Parameter{}, json: `{"name":"id","in":"query"}`},
		{name: "security", target: &SecurityScheme{}, json: `{"type":"http"}`},
		{name: "oauth flows", target: &OAuthFlows{}, json: `{"password":{"tokenUrl":"token","scopes":{"s":"v"}}}`},
		{name: "oauth flow", target: &OAuthFlow{}, json: `{"tokenUrl":"token","scopes":{"s":"v"}}`},
		{name: "example", target: &Example{}, json: `{"summary":"s"}`},
		{name: "server", target: &Server{}, json: `{"url":"http://example"}`},
		{name: "server variable", target: &ServerVariable{}, json: `{"default":"dev"}`},
		{name: "info", target: &Info{}, json: `{"title":"api","version":"1"}`},
		{name: "contact", target: &Contact{}, json: `{"name":"n"}`},
		{name: "license", target: &License{}, json: `{"name":"mit"}`},
		{name: "tag", target: &Tag{}, json: `{"name":"n"}`},
		{name: "path", target: &PathItem{}, json: `{"summary":"sum"}`},
		{name: "encoding", target: &Encoding{}, json: `{"contentType":"application/json"}`},
		{name: "request", target: &RequestBody{}, json: `{"description":"d"}`},
		{name: "external", target: &ExternalDocumentation{}, json: `{"url":"http://example"}`},
		{name: "response", target: &Response{}, json: `{"description":"ok"}`},
		{name: "media", target: &MediaType{}, json: `{"example":{"a":1}}`},
		{name: "operation", target: &Operation{}, json: `{"summary":"sum"}`},
		{name: "link", target: &Link{}, json: `{"operationId":"op"}`},
		{name: "schema", target: &Schema{}, json: `{"type":"object"}`},
		{name: "xml", target: &XML{}, json: `{"name":"node"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := json.Unmarshal([]byte(tt.json), tt.target); err != nil {
				t.Fatalf("unmarshal error: %v", err)
			}
		})
	}
}

func seedSession() context.Context {
	sessionCtx := NewSessionContext(context.Background())
	session := LookupSession(sessionCtx)
	session.Location = "main"
	session.RegisterComponents("main", &Components{
		Schemas:         Schemas{"Pet": {Type: "object"}},
		Parameters:      ParametersMap{"id": {Name: "id", In: "query"}},
		Headers:         Headers{"/components/headers/Trace": {Name: "Trace", In: "header"}},
		RequestBodies:   RequestBodies{"/components/requestBodies/Create": {Description: "create"}},
		Responses:       Responses{"/components/responses/Default": {Description: strPtr("default")}},
		SecuritySchemes: SecuritySchemes{"/components/securitySchemes/Bearer": {Type: "http"}},
		Examples:        Examples{"/components/examples/Sample": {Summary: "sample"}},
		Links:           Links{"/components/links/Self": {OperationID: "self"}},
		Callbacks:       Callbacks{"/components/callbacks/Event": {Ref: "inner"}},
	})
	return sessionCtx
}

func TestUnmarshalYAML_Table(t *testing.T) {
	tests := []struct {
		name      string
		target    yamlUnmarshaller
		source    interface{}
		ext       map[string]interface{}
		firstErr  error
		secondErr error
		wantErr   string
	}{
		{name: "components", target: &Components{}, source: Components{}, ext: map[string]interface{}{"x-a": 1}},
		{name: "parameter ref", target: &Parameter{}, source: Parameter{Ref: "#/components/parameters/id"}},
		{name: "security ref", target: &SecurityScheme{}, source: SecurityScheme{Ref: "#/components/securitySchemes/Bearer"}, ext: map[string]interface{}{"x-a": 1}},
		{name: "oauth flows", target: &OAuthFlows{}, source: OAuthFlows{}, ext: map[string]interface{}{"x-a": 1}},
		{name: "oauth flow", target: &OAuthFlow{}, source: OAuthFlow{}, ext: map[string]interface{}{"x-a": 1}},
		{name: "callback ref", target: &CallbackRef{}, source: CallbackRef{Ref: "#/components/callbacks/Event"}},
		{name: "example", target: &Example{}, source: Example{}, ext: map[string]interface{}{"x-a": 1}},
		{name: "server", target: &Server{}, source: Server{}, ext: map[string]interface{}{"x-a": 1}},
		{name: "server variable", target: &ServerVariable{}, source: ServerVariable{}, ext: map[string]interface{}{"x-a": 1}},
		{name: "info", target: &Info{}, source: Info{}, ext: map[string]interface{}{"x-a": 1}},
		{name: "contact", target: &Contact{}, source: Contact{}, ext: map[string]interface{}{"x-a": 1}},
		{name: "license", target: &License{}, source: License{}, ext: map[string]interface{}{"x-a": 1}},
		{name: "tag", target: &Tag{}, source: Tag{}, ext: map[string]interface{}{"x-a": 1}},
		{name: "path", target: &PathItem{}, source: PathItem{}, ext: map[string]interface{}{"x-a": 1}},
		{name: "encoding", target: &Encoding{}, source: Encoding{}, ext: map[string]interface{}{"x-a": 1}},
		{name: "request body ref", target: &RequestBody{}, source: RequestBody{Ref: "#/components/requestBodies/Create"}, ext: map[string]interface{}{"x-a": 1}},
		{name: "external doc", target: &ExternalDocumentation{}, source: ExternalDocumentation{}, ext: map[string]interface{}{"x-a": 1}},
		{name: "response ref", target: &Response{}, source: Response{Ref: "#/components/responses/Default"}, ext: map[string]interface{}{"x-a": 1}},
		{name: "media", target: &MediaType{}, source: MediaType{}, ext: map[string]interface{}{"x-a": 1}},
		{name: "operation", target: &Operation{}, source: nil, ext: map[string]interface{}{"x-a": 1}},
		{name: "link ref", target: &Link{}, source: Link{Ref: "#/components/links/Self"}, ext: map[string]interface{}{"x-a": 1}},
		{name: "schema ref", target: &Schema{}, source: Schema{Ref: "#/components/schemas/Pet"}, ext: map[string]interface{}{"x-a": 1}},
		{name: "xml", target: &XML{}, source: XML{}, ext: map[string]interface{}{"x-a": 1}},
		{name: "first decoder error", target: &XML{}, firstErr: errors.New("first decoder"), wantErr: "first decoder"},
		{name: "second decoder error", target: &XML{}, source: XML{}, secondErr: errors.New("second decoder"), wantErr: "second decoder"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := seedSession()
			err := tt.target.UnmarshalYAML(ctx, yamlDecoder(tt.source, tt.ext, tt.firstErr, tt.secondErr))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("expected err containing %q, got %v", tt.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func strPtr(v string) *string { return &v }
