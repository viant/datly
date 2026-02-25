package openapi3

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestResponsesHelpersAndOperationMarshal(t *testing.T) {
	responses := Responses{}
	SetResponse(responses, 200, &Response{Description: strPtr("ok")})
	SetResponse(responses, ResponseDefault, &Response{Description: strPtr("fallback")})
	SetResponse(responses, ResponseIntKey("201"), &Response{Description: strPtr("created")})

	if got, ok := GetResponse(responses, 200); !ok || got == nil || got.Description == nil || *got.Description != "ok" {
		t.Fatalf("expected integer-key lookup to resolve 200 response")
	}
	if _, ok := GetResponse(responses, "200"); !ok {
		t.Fatalf("expected string-key lookup to resolve 200 response")
	}
	if _, ok := GetResponse(responses, ResponseDefault); !ok {
		t.Fatalf("expected default response")
	}
	if got, ok := GetResponse(responses, ResponseIntKey("201")); !ok || got == nil || got.Description == nil || *got.Description != "created" {
		t.Fatalf("expected ResponseIntKey lookup to resolve 201 response")
	}
	if len(responses) != 3 {
		t.Fatalf("expected three responses to be set")
	}

	op := &Operation{
		Summary:   "sum",
		Responses: responses,
		Extension: Extension{"x-extra": true},
	}
	data, err := json.Marshal(op)
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}
	if !strings.Contains(string(data), "\"200\"") || !strings.Contains(string(data), "x-extra") {
		t.Fatalf("expected marshaled operation to include response and extension: %s", string(data))
	}
}

func assertNormalize[T ResponseKey](t *testing.T, name string, input T, expected ResponseCode) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		if got := NormalizeResponseCode(input); got != expected {
			t.Fatalf("expected %q, got %q", expected, got)
		}
	})
}

func TestNormalizeResponseCode_Table(t *testing.T) {
	assertNormalize(t, "string", "default", ResponseCode("default"))
	assertNormalize(t, "response code", ResponseDefault, ResponseCode("default"))
	assertNormalize(t, "response int key", ResponseIntKey("200"), ResponseCode("200"))
	assertNormalize(t, "int", int(200), ResponseCode("200"))
	assertNormalize(t, "int8", int8(101), ResponseCode("101"))
	assertNormalize(t, "int16", int16(202), ResponseCode("202"))
	assertNormalize(t, "int32", int32(203), ResponseCode("203"))
	assertNormalize(t, "int64", int64(204), ResponseCode("204"))
	assertNormalize(t, "uint", uint(205), ResponseCode("205"))
	assertNormalize(t, "uint8", uint8(206), ResponseCode("206"))
	assertNormalize(t, "uint16", uint16(207), ResponseCode("207"))
	assertNormalize(t, "uint32", uint32(208), ResponseCode("208"))
	assertNormalize(t, "uint64", uint64(209), ResponseCode("209"))
}

func TestUnmarshalYAMLErrorBranches_Table(t *testing.T) {
	tests := []struct {
		name      string
		target    yamlUnmarshaller
		source    interface{}
		firstErr  error
		secondErr error
		wantErr   string
	}{
		{name: "parameter first err", target: &Parameter{}, source: Parameter{}, firstErr: errors.New("p-first"), wantErr: "p-first"},
		{name: "link second err", target: &Link{}, source: Link{}, secondErr: errors.New("l-second"), wantErr: "l-second"},
		{name: "request body second err", target: &RequestBody{}, source: RequestBody{}, secondErr: errors.New("rb-second"), wantErr: "rb-second"},
		{name: "response second err", target: &Response{}, source: Response{}, secondErr: errors.New("resp-second"), wantErr: "resp-second"},
		{name: "security second err", target: &SecurityScheme{}, source: SecurityScheme{}, secondErr: errors.New("sec-second"), wantErr: "sec-second"},
		{name: "schema second err", target: &Schema{}, source: Schema{}, secondErr: errors.New("schema-second"), wantErr: "schema-second"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.target.UnmarshalYAML(context.Background(), yamlDecoder(tt.source, nil, tt.firstErr, tt.secondErr))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected err containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestOperationResponsesBoundary(t *testing.T) {
	t.Run("unmarshal json initializes responses", func(t *testing.T) {
		var op Operation
		if err := json.Unmarshal([]byte(`{"summary":"s"}`), &op); err != nil {
			t.Fatalf("unexpected unmarshal error: %v", err)
		}
		if op.Responses == nil {
			t.Fatalf("expected non-nil responses after unmarshal")
		}
	})

	t.Run("unmarshal yaml initializes responses", func(t *testing.T) {
		var op Operation
		err := op.UnmarshalYAML(context.Background(), yamlDecoder(map[string]interface{}{"summary": "s"}, map[string]interface{}{"x-a": 1}, nil, nil))
		if err != nil {
			t.Fatalf("unexpected yaml unmarshal error: %v", err)
		}
		if op.Responses == nil {
			t.Fatalf("expected non-nil responses after yaml unmarshal")
		}
	})
}
