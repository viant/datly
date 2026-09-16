package compiler

import (
	"context"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindinput "github.com/viant/bindly/input"
	requestprovider "github.com/viant/bindly/provider/request"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
)

func TestAuthoredInputDateFormat(t *testing.T) {
	type input struct {
		From *time.Time `parameter:",kind=form,in=from" format:"dateFormat=YYYY-MM-DD"`
		RFC  *time.Time `parameter:",kind=query,in=rfc"`
	}
	compiled, err := New(Input{Component: &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/dates"}}}, InputType: reflect.TypeFor[input]()}).Compile()
	require.NoError(t, err)
	route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/dates"})
	require.True(t, ok)
	for _, tc := range []struct {
		name, value     string
		invalid, absent bool
	}{
		{name: "date", value: "2026-09-15"},
		{name: "invalid", value: "2026-09-99", invalid: true},
		{name: "empty", invalid: true},
		{name: "absent", absent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			form := url.Values{}
			if !tc.absent {
				form.Set("from", tc.value)
			}
			providers := requestprovider.NewValues(requestprovider.WithForm(form), requestprovider.WithQuery(url.Values{"rfc": {"2026-09-15T00:00:00Z"}}))
			injector, err := bindly.NewInjector()
			require.NoError(t, err)
			scope, err := injector.ForScope(providers.Providers()...)
			require.NoError(t, err)
			var out input
			err = scope.Bind(context.Background(), &out, bindly.WithPlan(route.Plan()))
			if tc.invalid {
				var parse *time.ParseError
				require.ErrorAs(t, err, &parse)
				var binding *bindly.BindingError
				require.ErrorAs(t, err, &binding)
				require.Equal(t, 400, binding.StatusCode())
				return
			}
			require.NoError(t, err)
			if tc.absent {
				require.Nil(t, out.From)
			} else {
				require.Equal(t, "2026-09-15", out.From.Format("2006-01-02"))
			}
			require.Equal(t, "2026-09-15", out.RFC.Format("2006-01-02"))
		})
	}
	transform := bindingByPath(t, compiled.Bindings, "From").Transformer
	stamp := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	actual, err := transform.Transform(context.Background(), nil, &stamp)
	require.NoError(t, err)
	require.Equal(t, &stamp, actual)
}

func TestDateFormatErrorClassificationBySource(t *testing.T) {
	type input struct {
		Date *time.Time `format:"dateFormat=YYYY-MM-DD"`
	}
	field := reflect.TypeFor[input]().Field(0)
	for _, kind := range []string{"query", "path", "header", "cookie", "form", "body", "param", "component", "view", "const"} {
		t.Run(kind, func(t *testing.T) {
			binding := bindly.BindingSpec{Location: bindstate.Location{Kind: kind}}
			require.NoError(t, applyTimeFormat(field, &binding))
			_, err := binding.Transformer.Transform(context.Background(), nil, "2026-09-99")
			var parse *time.ParseError
			require.ErrorAs(t, err, &parse)
			var client *bindinput.Error
			switch kind {
			case "query", "path", "header", "cookie", "form", "body":
				require.ErrorAs(t, err, &client)
				require.Equal(t, 400, client.StatusCode())
			default:
				require.NotErrorAs(t, err, &client)
			}
		})
	}
}
