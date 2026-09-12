package openapi3

import (
	"encoding/json"
	"strings"
	"testing"
)

func decodeSequence(values ...interface{}) func(dest interface{}) error {
	index := 0
	return func(dest interface{}) error {
		if index >= len(values) {
			return nil
		}
		value := values[index]
		index++
		if err, ok := value.(error); ok {
			return err
		}
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		return json.Unmarshal(data, dest)
	}
}

func TestMarshalNoExtension_Table(t *testing.T) {
	trueVal := true
	tests := []struct {
		name  string
		value interface{}
	}{
		{name: "components", value: &Components{Schemas: Schemas{"Pet": {Type: "object"}}}},
		{name: "parameter", value: &Parameter{Name: "id", In: "query"}},
		{name: "security", value: &SecurityScheme{Type: "http"}},
		{name: "example", value: &Example{Summary: "s"}},
		{name: "server", value: &Server{URL: "http://example"}},
		{name: "server variable", value: &ServerVariable{Default: "dev"}},
		{name: "info", value: &Info{Title: "api", Version: "1.0"}},
		{name: "contact", value: &Contact{Name: "n"}},
		{name: "license", value: &License{Name: "mit"}},
		{name: "tag", value: &Tag{Name: "n"}},
		{name: "path item", value: &PathItem{Summary: "sum"}},
		{name: "encoding", value: &Encoding{ContentType: "application/json", Explode: &trueVal}},
		{name: "request body", value: &RequestBody{Description: "d"}},
		{name: "external", value: &ExternalDocumentation{URL: "http://example"}},
		{name: "response", value: &Response{Description: strPtr("ok")}},
		{name: "media", value: &MediaType{Example: map[string]interface{}{"a": 1}}},
		{name: "operation", value: &Operation{Summary: "sum", Responses: Responses{}}},
		{name: "link", value: &Link{OperationID: "op"}},
		{name: "schema", value: &Schema{Type: "object"}},
		{name: "xml", value: &XML{Name: "node"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.value)
			if err != nil {
				t.Fatalf("unexpected marshal error: %v", err)
			}
			if strings.Contains(string(data), "x-") {
				t.Fatalf("did not expect extension key in %s", string(data))
			}
		})
	}
}

func TestYAMLRefAndNonRefBranches(t *testing.T) {
	ctx := seedSession()

	t.Run("parameter non ref and ref", func(t *testing.T) {
		var nonRef Parameter
		if err := nonRef.UnmarshalYAML(ctx, decodeSequence(Parameter{Name: "id", In: "query"})); err != nil {
			t.Fatalf("unexpected non-ref error: %v", err)
		}

		var refHit Parameter
		if err := refHit.UnmarshalYAML(ctx, decodeSequence(map[string]interface{}{"$ref": "#/components/parameters/id"})); err != nil {
			t.Fatalf("unexpected ref lookup error: %v", err)
		}
	})

	t.Run("link non ref and ref", func(t *testing.T) {
		var nonRef Link
		if err := nonRef.UnmarshalYAML(ctx, decodeSequence(Link{OperationID: "op"}, map[string]interface{}{"x-a": 1})); err != nil {
			t.Fatalf("unexpected non-ref error: %v", err)
		}

		var refHit Link
		if err := refHit.UnmarshalYAML(ctx, decodeSequence(map[string]interface{}{"$ref": "#/components/links/Self"}, map[string]interface{}{"x-a": 1})); err != nil {
			t.Fatalf("unexpected ref lookup error: %v", err)
		}
	})

	t.Run("request body non ref and ref", func(t *testing.T) {
		var nonRef RequestBody
		if err := nonRef.UnmarshalYAML(ctx, decodeSequence(RequestBody{Description: "d"}, map[string]interface{}{"x-a": 1})); err != nil {
			t.Fatalf("unexpected non-ref error: %v", err)
		}

		var refHit RequestBody
		if err := refHit.UnmarshalYAML(ctx, decodeSequence(map[string]interface{}{"$ref": "#/components/requestBodies/Create"}, map[string]interface{}{"x-a": 1})); err != nil {
			t.Fatalf("unexpected ref lookup error: %v", err)
		}
	})

	t.Run("response non ref and ref", func(t *testing.T) {
		var nonRef Response
		if err := nonRef.UnmarshalYAML(ctx, decodeSequence(Response{Description: strPtr("ok")}, map[string]interface{}{"x-a": 1})); err != nil {
			t.Fatalf("unexpected non-ref error: %v", err)
		}

		var refHit Response
		if err := refHit.UnmarshalYAML(ctx, decodeSequence(map[string]interface{}{"$ref": "#/components/responses/Default"}, map[string]interface{}{"x-a": 1})); err != nil {
			t.Fatalf("unexpected ref lookup error: %v", err)
		}
	})

	t.Run("security non ref and ref", func(t *testing.T) {
		var nonRef SecurityScheme
		if err := nonRef.UnmarshalYAML(ctx, decodeSequence(SecurityScheme{Type: "http"}, map[string]interface{}{"x-a": 1})); err != nil {
			t.Fatalf("unexpected non-ref error: %v", err)
		}

		var refHit SecurityScheme
		if err := refHit.UnmarshalYAML(ctx, decodeSequence(map[string]interface{}{"$ref": "#/components/securitySchemes/Bearer"}, map[string]interface{}{"x-a": 1})); err != nil {
			t.Fatalf("unexpected ref lookup error: %v", err)
		}
	})

	t.Run("schema non ref and ref", func(t *testing.T) {
		var nonRef Schema
		if err := nonRef.UnmarshalYAML(ctx, decodeSequence(Schema{Type: "object"}, map[string]interface{}{"x-a": 1})); err != nil {
			t.Fatalf("unexpected non-ref error: %v", err)
		}

		var refHit Schema
		if err := refHit.UnmarshalYAML(ctx, decodeSequence(map[string]interface{}{"$ref": "#/components/schemas/Pet"}, map[string]interface{}{"x-a": 1})); err != nil {
			t.Fatalf("unexpected ref lookup error: %v", err)
		}
	})
}

func TestSessionLookupMissingBranches(t *testing.T) {
	s := NewSession()
	s.Location = "loc"
	s.RegisterComponents("loc", &Components{
		Schemas:         Schemas{"Pet": {Type: "object"}},
		Parameters:      ParametersMap{"id": {Name: "id", In: "query"}},
		Headers:         Headers{"/components/headers/Trace": {Name: "Trace", In: "header"}},
		RequestBodies:   RequestBodies{"/components/requestBodies/Create": {Description: "create"}},
		Responses:       Responses{"/components/responses/Default": {Description: strPtr("default")}},
		SecuritySchemes: SecuritySchemes{"/components/securitySchemes/Bearer": {Type: "http"}},
		Examples:        Examples{"/components/examples/Sample": {Summary: "sample"}},
		Links:           Links{"/components/links/Self": {OperationID: "self"}},
		Callbacks:       Callbacks{"/components/callbacks/Event": {Ref: "eventRef"}},
	})

	tests := []struct {
		name    string
		lookup  func() error
		wantErr string
	}{
		{name: "parameter missing location", lookup: func() error { _, err := s.LookupParameter("other", "#/components/parameters/id"); return err }, wantErr: "failed to lookup location"},
		{name: "header missing value", lookup: func() error { _, err := s.LookupHeaders("loc", "#/components/headers/Other"); return err }, wantErr: "failed to lookup"},
		{name: "request missing value", lookup: func() error { _, err := s.LookupRequestBody("loc", "#/components/requestBodies/Other"); return err }, wantErr: "failed to lookup"},
		{name: "response missing value", lookup: func() error { _, err := s.LookupResponse("loc", "#/components/responses/Other"); return err }, wantErr: "failed to lookup"},
		{name: "security missing value", lookup: func() error {
			_, err := s.LookupSecurityScheme("loc", "#/components/securitySchemes/Other")
			return err
		}, wantErr: "failed to lookup"},
		{name: "example missing value", lookup: func() error { _, err := s.LookupExample("loc", "#/components/examples/Other"); return err }, wantErr: "failed to lookup"},
		{name: "link missing value", lookup: func() error { _, err := s.LookupLink("loc", "#/components/links/Other"); return err }, wantErr: "failed to lookup"},
		{name: "callback missing value", lookup: func() error { _, err := s.LookupCallback("loc", "#/components/callbacks/Other"); return err }, wantErr: "failed to lookup"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.lookup()
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected err containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}
