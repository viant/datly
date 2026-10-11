package runtime

import (
	"context"
	"net/http"
	"net/url"
	"reflect"
	"sync"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/constant"
	"github.com/viant/datly/internal/testharness"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/spec"
)

type constantComponentLoader struct{ registration *RegisteredComponent }

func (l constantComponentLoader) LoadComponent(context.Context, spec.Key) (*RegisteredComponent, error) {
	return l.registration, nil
}

func TestIndexedRuntimeBindsCanonicalConstantsInHandler(t *testing.T) {
	type input struct {
		Table string `parameter:"Table,kind=const,in=Table,value=authored"`
	}
	type output struct{ Table string }
	for _, override := range []string{"", "instance"} {
		t.Run("override="+override, func(t *testing.T) {
			component := &spec.Component{
				Key:    spec.Key{Kind: spec.KindComponent, Scope: "example.com/lazy", Name: "Constants"},
				Routes: []*spec.Route{{Method: http.MethodGet, Path: "/lazy-constants"}},
			}
			var values *constant.Values
			want := "authored"
			if override != "" {
				var err error
				values, err = constant.New(map[string]string{"Table": override})
				if err != nil {
					t.Fatal(err)
				}
				want = override
			}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
				Component: component, Const: values, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), HandlerOwnedOutput: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			registration := &RegisteredComponent{
				Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(output{}),
				Handler: rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
					var dependencies struct {
						SQLConst0 string `parameter:"SQLConst0,kind=const,in=Table,required=true"`
					}
					if err := inv.Binder.Bind(ctx, &dependencies); err != nil {
						return nil, err
					}
					return &output{Table: dependencies.SQLConst0}, nil
				}),
			}
			runtime, err := NewIndexedRuntime([]*spec.Component{artifact.Component}, nil, constantComponentLoader{registration})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := runtime.Shutdown(context.Background()); err != nil {
					t.Error(err)
				}
			})
			var wg sync.WaitGroup
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					request := testharness.NewRequest(http.MethodGet, "/lazy-constants").WithQuery(url.Values{"Table": {"client"}, "SQLConst0": {"client"}})
					actual, err := executeTestRoute(t, runtime, context.Background(), request)
					if err != nil {
						t.Error(err)
						return
					}
					if actual.(*output).Table != want {
						t.Errorf("table = %q, want %q", actual.(*output).Table, want)
					}
				}()
			}
			wg.Wait()
		})
	}
}
