package compiler

import (
	"context"
	"errors"
	"github.com/viant/bindly"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/spec"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

type formattedBodyRow struct {
	When     *time.Time `format:"timeLayout=2006-01-02T15:04:05Z07"`
	Day      *time.Time `format:"dateFormat=YYYY-MM-DD"`
	Disabled *bool
	Child    *formattedBodyChild
	Has      *formattedBodyHas `setMarker:"true" json:"-"`
}
type formattedBodyChild struct {
	When *time.Time           `format:"timeLayout=2006-01-02T15:04:05Z07"`
	Has  *struct{ When bool } `setMarker:"true" json:"-"`
}
type formattedBodyHas struct{ When, Day, Disabled, Child bool }
type formattedBodyInput struct {
	Rows []*formattedBodyRow `parameter:"Rows,kind=body,in=data"`
}

func TestNestedAuthoredBodyFormatsAndPresence(t *testing.T) {
	compiled, err := New(Input{Component: &spec.Component{Routes: []*spec.Route{{Method: "PATCH", Path: "/rows"}}}, InputType: reflect.TypeFor[formattedBodyInput]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/rows"})
	if !ok {
		t.Fatal("missing route")
	}
	if route.Fields()[0].SourceType() != reflect.TypeFor[[]*formattedBodyRow]() {
		t.Fatal("formatted body lost its original public JSON contract")
	}
	for _, tc := range []struct {
		description, input   string
		expectedWhen         string
		presentWhen, invalid bool
	}{
		{"short UTC", "{\"data\":[{\"when\":\"2026-10-01T04:20:27+00\",\"disabled\":false}]}", "2026-10-01T04:20:27Z", true, false},
		{"RFC UTC", `{"data":[{"when":"2026-10-01T04:20:27Z"}]}`, "2026-10-01T04:20:27Z", true, false},
		{"RFC fractional nonhour timezone", `{"data":[{"when":"2026-10-01T09:50:27.123456789+05:30"}]}`, "2026-10-01T04:20:27.123456789Z", true, false},
		{"null remains present", `{"data":[{"when":null,"disabled":false}]}`, "", true, false},
		{"omission remains absent", `{"data":[{"disabled":false,"Has":{"When":true}}]}`, "", false, false},
		{"bad authored date", `{"data":[{"when":"2026-99-01T04:20:27+00"}]}`, "", false, true},
		{"invalid numeric date", `{"data":[{"when":123}]}`, "", false, true},
		{"malformed JSON", `{"data":[{"when":"2026-10-01T04:20:27+00" "disabled":false}]}`, "", false, true},
	} {
		t.Run(tc.description, func(t *testing.T) {
			request := httptest.NewRequest("PATCH", "/rows", strings.NewReader(tc.input))
			request.Header.Set("Content-Type", "application/json")
			providers, err := requestprovider.New(request)
			if err != nil {
				t.Fatal(err)
			}
			defer providers.Close()
			injector, err := bindly.NewInjector()
			if err != nil {
				t.Fatal(err)
			}
			scope, err := injector.ForScope(providers.Providers()...)
			if err != nil {
				t.Fatal(err)
			}
			var actual formattedBodyInput
			err = scope.Bind(t.Context(), &actual, bindly.WithPlan(route.Plan()))
			if tc.invalid {
				var classified interface{ StatusCode() int }
				if err == nil || !errors.As(err, &classified) || classified.StatusCode() != 400 {
					t.Fatalf("expected classified input400, actual=%v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			row := actual.Rows[0]
			if row.Has == nil || row.Has.When != tc.presentWhen {
				t.Fatalf("presence=%+v", row.Has)
			}
			if tc.expectedWhen != "" {
				if row.When == nil || row.When.UTC().Format(time.RFC3339Nano) != tc.expectedWhen {
					t.Fatalf("when=%v expected=%s", row.When, tc.expectedWhen)
				}
			} else if row.When != nil {
				t.Fatal("nil/omitted date changed")
			}
			if strings.Contains(tc.input, `"disabled":false`) && (row.Disabled == nil || *row.Disabled || !row.Has.Disabled) {
				t.Fatal("explicit false lost value/presence")
			}
		})
	}
	input := `{"data":[{"when":"2026-10-01T04:20:27+00","day":"2026-10-01","child":{"when":"2026-10-01T09:50:27+05:30"}},{"when":null,"child":null}]}`
	request := httptest.NewRequest("PATCH", "/rows", strings.NewReader(input))
	request.Header.Set("Content-Type", "application/json")
	providers, err := requestprovider.New(request)
	if err != nil {
		t.Fatal(err)
	}
	defer providers.Close()
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	scope, err := injector.ForScope(providers.Providers()...)
	if err != nil {
		t.Fatal(err)
	}
	var actual formattedBodyInput
	if err := scope.Bind(context.Background(), &actual, bindly.WithPlan(route.Plan())); err != nil {
		t.Fatal(err)
	}
	if len(actual.Rows) != 2 || actual.Rows[0].Day == nil || actual.Rows[0].Child == nil || actual.Rows[0].Child.When.UTC().Format(time.RFC3339) != "2026-10-01T04:20:27Z" || !actual.Rows[0].Child.Has.When || actual.Rows[1].Child != nil || !actual.Rows[1].Has.Child {
		t.Fatalf("nested/multiple row result=%+v", actual)
	}
}

func TestBodyWithoutAuthoredFormatKeepsOrdinaryDecoder(t *testing.T) {
	type plain struct{ When *time.Time }
	type input struct {
		Rows []*plain `parameter:"Rows,kind=body,in=data"`
	}
	compiled, err := New(Input{Component: &spec.Component{Routes: []*spec.Route{{Method: "PATCH", Path: "/plain"}}}, InputType: reflect.TypeFor[input]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	route, _ := compiled.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/plain"})
	if route.Fields()[0].SourceType() != reflect.TypeFor[[]*plain]() {
		t.Fatal("ordinary body source type changed")
	}
	request := httptest.NewRequest("PATCH", "/plain", strings.NewReader(`{"data":[{"when":"2026-10-01T04:20:27+00"}]}`))
	request.Header.Set("Content-Type", "application/json")
	providers, err := requestprovider.New(request)
	if err != nil {
		t.Fatal(err)
	}
	defer providers.Close()
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	scope, err := injector.ForScope(providers.Providers()...)
	if err != nil {
		t.Fatal(err)
	}
	var actual input
	if err := scope.Bind(t.Context(), &actual, bindly.WithPlan(route.Plan())); err == nil {
		t.Fatal("unauthored short date must not silently gain parsing behavior")
	}
}

type bodyFormatDuplicateFixture struct {
	When  *time.Time `format:"timeLayout=2006-01-02T15:04:05Z07"`
	Count int64      `json:"count"`
}

func TestBodyFormatRetainsDuplicateOrderAndIntegerPrecision(t *testing.T) {
	for _, tc := range []struct {
		description, input string
		expected           int64
	}{
		{"same key last wins", `{"count":1,"count":2,"when":"2026-10-01T04:20:27+00"}`, 2},
		{"case variant order reverses", `{"Count":2,"count":1,"when":"2026-10-01T04:20:27+00"}`, 1},
		{"large integer stays exact", `{"count":9007199254740993,"when":"2026-10-01T04:20:27+00"}`, 9007199254740993},
	} {
		t.Run(tc.description, func(t *testing.T) {
			actual := reviewTransform(t, reflect.TypeFor[*bodyFormatDuplicateFixture](), tc.input).(*bodyFormatDuplicateFixture)
			if actual.Count != tc.expected {
				t.Fatalf("count=%d expected=%d", actual.Count, tc.expected)
			}
		})
	}
}

type bodyFormatShadowedEmbedded struct {
	When *time.Time `format:"timeLayout=2006-01-02T15:04:05Z07"`
}
type bodyFormatDominantField struct {
	bodyFormatShadowedEmbedded
	When  string
	Other *time.Time `format:"timeLayout=2006-01-02T15:04:05Z07"`
}

func TestBodyFormatJSONEmbeddingDominance(t *testing.T) {
	actual := reviewTransform(t, reflect.TypeFor[*bodyFormatDominantField](), `{"when":"not-a-date","other":"2026-10-01T04:20:27+00"}`).(*bodyFormatDominantField)
	if actual.When != "not-a-date" || actual.bodyFormatShadowedEmbedded.When != nil || actual.Other == nil {
		t.Fatal("embedding dominance changed ordinary JSON field authority")
	}
}
