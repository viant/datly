package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

type frameworkAlwaysValid struct{ calls int }

func (v *frameworkAlwaysValid) Validate(context.Context, any, ...any) (*xhandler.Validation, error) {
	v.calls++
	return &xhandler.Validation{}, nil
}

type frameworkNullRow struct {
	ID    int  `sqlx:"id,primaryKey"`
	Value *int `sqlx:"value,required"`
}

func TestFrameworkValidatorProtectedSQLite(t *testing.T) {
	for _, mode := range []string{"parent replacement", "custom capability", "component replacement", "missing data"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			fake := &frameworkAlwaysValid{}
			injector, err := bindly.NewInjector(bindly.WithProviders(handlerprovider.Static(xhandler.FrameworkValidatorKey, fake)))
			if err != nil {
				t.Fatal(err)
			}
			request := Request{Injector: injector, Input: testRouteInput(t, reflect.TypeOf(struct{}{})), DataSource: dml.Source{DB: h.DB}, Capabilities: rhandler.InvocationCapabilities{Validator: fake}}
			if mode == "component replacement" {
				request.Providers = []locator.Provider{handlerprovider.Static(xhandler.FrameworkValidatorKey, fake)}
			}
			if mode == "missing data" {
				request.DataSource = nil
			}
			afterValidation := false
			request.Handler = rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
				if mode == "custom capability" {
					ordinary, found, err := invocation.Binder.Lookup(ctx, xhandler.ValidatorKey)
					if err != nil || !found || ordinary != fake {
						return nil, fmt.Errorf("ordinary custom validator changed: %v", err)
					}
				}
				resolved, found, err := invocation.Binder.Lookup(ctx, xhandler.FrameworkValidatorKey)
				if err != nil {
					return nil, err
				}
				if !found {
					return nil, fmt.Errorf("framework capability missing")
				}
				result, err := resolved.(xhandler.Validator).Validate(ctx, &frameworkNullRow{}, xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true})
				if err != nil {
					return nil, err
				}
				if err = result.Err(); err != nil {
					return nil, err
				}
				afterValidation = true
				return nil, nil
			})
			_, err = New().Execute(ctx, request)
			if err == nil {
				t.Fatal("validation unexpectedly passed")
			}
			switch mode {
			case "component replacement":
				if !strings.Contains(err.Error(), "protected runtime kind") {
					t.Fatalf("got %v", err)
				}
			case "missing data":
				if !strings.Contains(err.Error(), "data source") {
					t.Fatalf("got %v", err)
				}
			default:
				var typed *xhandler.Validation
				if !errors.As(err, &typed) || typed.StatusCode() != 422 {
					t.Fatalf("typed validation lost: %v", err)
				}
			}
			if fake.calls != 0 || afterValidation {
				t.Fatalf("replacement/later phase ran: fake=%d later=%v", fake.calls, afterValidation)
			}
		})
	}
}
