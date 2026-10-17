package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

type xlsContextKey struct{}
type xlsBodyProbe struct {
	Value        string
	ContextValue string
}

func (b *xlsBodyProbe) UnmarshalXLS(ctx context.Context, raw []byte) error {
	if string(raw) != "workbook" {
		return fmt.Errorf("invalid workbook")
	}
	b.Value = string(raw)
	b.ContextValue, _ = ctx.Value(xlsContextKey{}).(string)
	return nil
}

type jsonBodyProbe struct {
	Value   string
	Decoded bool
}

func (b *jsonBodyProbe) UnmarshalJSON(raw []byte) error {
	var v struct{ Value string }
	if err := json.Unmarshal(raw, &v); err != nil {
		return err
	}
	b.Value = v.Value
	b.Decoded = true
	return nil
}
func TestHTTPXLSBodyScope(t *testing.T) {
	for _, deferred := range []bool{false, true} {
		t.Run(fmt.Sprint(deferred), func(t *testing.T) {
			for _, tc := range []struct {
				name, raw, media string
				target           reflect.Type
				wantError, found bool
			}{
				{"xlsx", "workbook", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet; charset=utf-8", reflect.TypeFor[*xlsBodyProbe](), false, true},
				{"malformed", "bad", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", reflect.TypeFor[*xlsBodyProbe](), true, false},
				{"unsupported", "workbook", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", reflect.TypeFor[*struct{ Value string }](), true, false},
				{"empty", "", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", reflect.TypeFor[*xlsBodyProbe](), false, false},
				{"json", `{"Value":"json"}`, "application/json", reflect.TypeFor[*jsonBodyProbe](), false, true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					req := httptest.NewRequest("PATCH", "/records", strings.NewReader(tc.raw))
					req.Header.Set("Content-Type", tc.media)
					req = req.WithContext(context.WithValue(req.Context(), xlsContextKey{}, "original"))
					scope, err := newHTTPRequestScope(req, nil, deferred)
					if err != nil {
						t.Fatal(err)
					}
					defer scope.Close()
					value, found, err := scope.Body().Value(req.Context(), tc.target, "")
					if (err != nil) != tc.wantError || found != tc.found {
						t.Fatalf("value=%T found=%v err=%v", value, found, err)
					}
					if tc.name == "xlsx" && (value.(*xlsBodyProbe).Value != "workbook" || value.(*xlsBodyProbe).ContextValue != "original") {
						t.Fatal("decoder/context lost")
					}
					if tc.name == "json" && (!value.(*jsonBodyProbe).Decoded || value.(*jsonBodyProbe).Value != "json") {
						t.Fatal("JSON decoder changed")
					}
				})
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			req := httptest.NewRequest("PATCH", "/records", strings.NewReader("workbook")).WithContext(ctx)
			req.Header.Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
			scope, err := newHTTPRequestScope(req, nil, deferred)
			if err != nil {
				t.Fatal(err)
			}
			defer scope.Close()
			_, _, err = scope.Body().Value(ctx, reflect.TypeFor[*xlsBodyProbe](), "")
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation lost: %v", err)
			}
		})
	}
}
