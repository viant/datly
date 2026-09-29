package openapi3

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestSessionRegisterAndLookup_Table(t *testing.T) {
	s := NewSession()
	s.Location = "loc"
	s.RegisterComponents("loc", &Components{
		Schemas:         Schemas{"Pet": {Type: "object"}},
		Parameters:      ParametersMap{"id": {Name: "id", In: "query"}},
		Headers:         Headers{"/components/headers/Trace": {Name: "Trace", In: "header"}},
		RequestBodies:   RequestBodies{"/components/requestBodies/Create": {Description: "create"}},
		Responses:       Responses{"/components/responses/Default": {Description: stringRef("default")}},
		SecuritySchemes: SecuritySchemes{"/components/securitySchemes/Bearer": {Type: "http"}},
		Examples:        Examples{"/components/examples/Sample": {Summary: "sample"}},
		Links:           Links{"/components/links/Self": {OperationID: "self"}},
		Callbacks:       Callbacks{"/components/callbacks/Event": {Ref: "eventRef"}},
	})

	tests := []struct {
		name    string
		lookup  func() (interface{}, error)
		wantErr string
		assert  func(t *testing.T, got interface{})
	}{
		{name: "lookup schema", lookup: func() (interface{}, error) { return s.LookupSchema("loc", "#/components/schemas/Pet") }, assert: func(t *testing.T, got interface{}) {
			if got.(*Schema).Ref == "" {
				t.Fatalf("missing ref")
			}
		}},
		{name: "lookup parameter", lookup: func() (interface{}, error) { return s.LookupParameter("loc", "#/components/parameters/id") }, assert: func(t *testing.T, got interface{}) {
			if got.(*Parameter).Name != "id" {
				t.Fatalf("name mismatch")
			}
		}},
		{name: "lookup header", lookup: func() (interface{}, error) { return s.LookupHeaders("loc", "#/components/headers/Trace") }, assert: func(t *testing.T, got interface{}) {
			if got.(*Parameter).In != "header" {
				t.Fatalf("in mismatch")
			}
		}},
		{name: "lookup request body", lookup: func() (interface{}, error) { return s.LookupRequestBody("loc", "#/components/requestBodies/Create") }, assert: func(t *testing.T, got interface{}) {
			if got.(*RequestBody).Description != "create" {
				t.Fatalf("desc mismatch")
			}
		}},
		{name: "lookup response", lookup: func() (interface{}, error) { return s.LookupResponse("loc", "#/components/responses/Default") }, assert: func(t *testing.T, got interface{}) {
			if got.(*Response).Description == nil {
				t.Fatalf("desc missing")
			}
		}},
		{name: "lookup security", lookup: func() (interface{}, error) {
			return s.LookupSecurityScheme("loc", "#/components/securitySchemes/Bearer")
		}, assert: func(t *testing.T, got interface{}) {
			if got.(*SecurityScheme).Type != "http" {
				t.Fatalf("type mismatch")
			}
		}},
		{name: "lookup example", lookup: func() (interface{}, error) { return s.LookupExample("loc", "#/components/examples/Sample") }, assert: func(t *testing.T, got interface{}) {
			if got.(*Example).Summary != "sample" {
				t.Fatalf("summary mismatch")
			}
		}},
		{name: "lookup link", lookup: func() (interface{}, error) { return s.LookupLink("loc", "#/components/links/Self") }, assert: func(t *testing.T, got interface{}) {
			if got.(*Link).OperationID != "self" {
				t.Fatalf("op mismatch")
			}
		}},
		{name: "lookup callback", lookup: func() (interface{}, error) { return s.LookupCallback("loc", "#/components/callbacks/Event") }, assert: func(t *testing.T, got interface{}) {
			if got.(*CallbackRef).Ref != "#/components/callbacks/Event" {
				t.Fatalf("ref mismatch")
			}
		}},
		{name: "missing location", lookup: func() (interface{}, error) { return s.LookupSchema("other", "#/components/schemas/Pet") }, wantErr: "failed to lookup location"},
		{name: "missing value", lookup: func() (interface{}, error) { return s.LookupParameter("loc", "#/components/parameters/other") }, wantErr: "failed to lookup"},
		{name: "unsupported ref", lookup: func() (interface{}, error) { return s.LookupSchema("loc", "./components/schemas/Pet") }, wantErr: "unsupported"},
		{name: "unsupported parameter ref", lookup: func() (interface{}, error) { return s.LookupParameter("loc", "./components/parameters/id") }, wantErr: "unsupported"},
		{name: "unsupported header ref", lookup: func() (interface{}, error) { return s.LookupHeaders("loc", "./components/headers/Trace") }, wantErr: "unsupported"},
		{name: "unsupported request body ref", lookup: func() (interface{}, error) { return s.LookupRequestBody("loc", "./components/requestBodies/Create") }, wantErr: "unsupported"},
		{name: "unsupported response ref", lookup: func() (interface{}, error) { return s.LookupResponse("loc", "./components/responses/Default") }, wantErr: "unsupported"},
		{name: "unsupported security ref", lookup: func() (interface{}, error) {
			return s.LookupSecurityScheme("loc", "./components/securitySchemes/Bearer")
		}, wantErr: "unsupported"},
		{name: "unsupported example ref", lookup: func() (interface{}, error) { return s.LookupExample("loc", "./components/examples/Sample") }, wantErr: "unsupported"},
		{name: "unsupported link ref", lookup: func() (interface{}, error) { return s.LookupLink("loc", "./components/links/Self") }, wantErr: "unsupported"},
		{name: "unsupported callback ref", lookup: func() (interface{}, error) { return s.LookupCallback("loc", "./components/callbacks/Event") }, wantErr: "unsupported"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.lookup()
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.assert != nil {
				tt.assert(t, got)
			}
		})
	}
}

func TestSessionHelpers(t *testing.T) {
	t.Run("add defer and close", func(t *testing.T) {
		s := NewSession()
		order := 0
		s.AddDefer(func() error { order++; return nil })
		s.AddDefer(func() error { order++; return nil })
		if err := s.Close(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if order != 2 {
			t.Fatalf("expected both defers, got %d", order)
		}
	})

	t.Run("close returns defer error", func(t *testing.T) {
		s := NewSession()
		s.AddDefer(func() error { return errors.New("boom") })
		if err := s.Close(); err == nil || err.Error() != "boom" {
			t.Fatalf("expected boom, got %v", err)
		}
	})

	t.Run("normalize ref", func(t *testing.T) {
		s := NewSession()
		if got := s.normalizeRef("/components/schemas/Pet", "/components/schemas/"); got != "Pet" {
			t.Fatalf("unexpected normalize result: %s", got)
		}
	})

	t.Run("lookup session from context", func(t *testing.T) {
		ctx := NewSessionContext(context.Background())
		if LookupSession(ctx) == nil {
			t.Fatalf("expected session in context")
		}
		if LookupSession(context.Background()) != nil {
			t.Fatalf("expected nil session")
		}
	})
}

func stringRef(v string) *string { return &v }
