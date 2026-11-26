package locator

import (
	"encoding/json"
	"errors"
	hstate "github.com/viant/xdatly/handler/state"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestBodyMapWithoutContractKeepsQueryAndFormPropagation(t *testing.T) {
	for _, name := range []string{"", "ignored"} {
		request := httptest.NewRequest("POST", "http://localhost/items?keep=url&name=url", strings.NewReader(`{"name":"body","count":2,"enabled":false}`))
		form := hstate.NewForm()
		typ := reflect.TypeFor[map[string]any]()
		var destinations []reflect.Type
		body, err := NewBody(WithRequest(request), WithBodyType(typ), WithForm(form), WithUnmarshal(func(raw []byte, dest interface{}) error {
			destinations = append(destinations, reflect.TypeOf(dest))
			return json.Unmarshal(raw, dest)
		}))
		if err != nil {
			t.Fatal(err)
		}
		result, present, err := body.Value(t.Context(), typ, name)
		if err != nil || !present {
			t.Fatalf("%v %t", err, present)
		}
		mapped, ok := result.(*map[string]any)
		if !ok || (*mapped)["name"] != "body" || (*mapped)["count"] != float64(2) {
			t.Fatalf("returned %T: %v", result, result)
		}
		if len(destinations) != 1 || destinations[0] != reflect.PointerTo(typ) {
			t.Fatalf("decoder destinations: %v", destinations)
		}
		query := request.URL.Query()
		if query.Get("keep") != "url" || query.Get("name") != "body" || query.Get("count") != "2" || query.Get("enabled") != "false" {
			t.Fatalf("query: %v", query)
		}
		// The same legacy body values also feed the form source.
		value := form.Get("name")
		if value != "body" {
			t.Fatalf("form: %v", value)
		}
	}
}
func TestBodyMapWithoutContractPreservesStringMapAndErrors(t *testing.T) {
	typ := reflect.TypeFor[map[string]string]()
	for _, tc := range []struct {
		body        string
		wantPresent bool
		wantError   bool
	}{
		{`{"name":"body"}`, true, false}, {`{}`, true, false}, {`null`, true, false}, {``, false, false}, {`{`, false, true},
	} {
		t.Run(tc.body, func(t *testing.T) {
			req := httptest.NewRequest("POST", "http://localhost/?keep=url", strings.NewReader(tc.body))
			locator, err := NewBody(WithRequest(req), WithBodyType(typ), WithForm(hstate.NewForm()), WithUnmarshal(json.Unmarshal))
			if err != nil {
				t.Fatal(err)
			}
			value, present, err := locator.Value(t.Context(), typ, "")
			if present != tc.wantPresent || (err != nil) != tc.wantError {
				t.Fatalf("%T present=%t err=%v", value, present, err)
			}
			if present {
				if _, ok := value.(*map[string]string); !ok {
					t.Fatalf("type %T", value)
				}
			}
			if req.URL.Query().Get("keep") != "url" {
				t.Fatal("query lost")
			}
		})
	}
	sentinel := errors.New("configured decoder failed")
	req := httptest.NewRequest("POST", "http://localhost/?name=original", strings.NewReader(`{"name":"new"}`))
	locator, _ := NewBody(WithRequest(req), WithBodyType(typ), WithForm(hstate.NewForm()), WithUnmarshal(func([]byte, interface{}) error { return sentinel }))
	_, present, err := locator.Value(t.Context(), typ, "")
	if present || !errors.Is(err, sentinel) || req.URL.Query().Get("name") != "original" {
		t.Fatal("failed decoder propagated body data")
	}
}
