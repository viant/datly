package compiler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/viant/bindly/state"
	xcodec "github.com/viant/xdatly/codec"
)

type invocationLookupCodec struct{}

func (invocationLookupCodec) Value(ctx context.Context, raw any, opts ...xcodec.Option) (any, error) {
	lookup := xcodec.NewOptions(opts).LookupValue
	if lookup == nil {
		return nil, fmt.Errorf("invocation value lookup unavailable")
	}
	suffix, err := lookup(ctx, "Delimiter")
	if err != nil {
		return nil, err
	}
	return raw.(string) + suffix.(string), nil
}

type valueResolver struct {
	wantContext context.Context
	value       any
	found       bool
	err         error
}

func (r valueResolver) Value(ctx context.Context, where *state.Location) (any, bool, error) {
	if ctx != r.wantContext {
		return nil, false, fmt.Errorf("codec lookup lost invocation context")
	}
	if where.Kind != "param" || where.In != "Delimiter" {
		return nil, false, fmt.Errorf("wrong parameter location: %+v", where)
	}
	return r.value, r.found, r.err
}
func TestParameterCodecLookupUsesInvocationResolver(t *testing.T) {
	transform := newCodecTransformer(invocationLookupCodec{})
	for _, delimiter := range []string{"\t", ",", "|"} {
		t.Run(fmt.Sprintf("delimiter%q", delimiter), func(t *testing.T) {
			t.Parallel()
			ctx := context.WithValue(context.Background(), struct{}{}, delimiter)
			for i := 0; i < 100; i++ {
				got, err := transform.Transform(ctx, valueResolver{wantContext: ctx, value: delimiter, found: true}, "url")
				if err != nil || got != "url"+delimiter {
					t.Fatalf("codec invocation result %v,%v", got, err)
				}
			}
		})
	}
}
func TestParameterCodecLookupRetainsMissingAndResolverFailures(t *testing.T) {
	transform := newCodecTransformer(invocationLookupCodec{})
	ctx := context.Background()
	cause := errors.New("dependency resolution failed")
	if _, err := transform.Transform(ctx, valueResolver{wantContext: ctx, err: cause}, "url"); !errors.Is(err, cause) {
		t.Fatalf("resolver failure lost: %v", err)
	}
	for _, resolver := range []valueResolver{{wantContext: ctx}, {wantContext: ctx, value: ",", found: false}} {
		if _, err := transform.Transform(ctx, resolver, "url"); err == nil || !strings.Contains(err.Error(), "Delimiter") {
			t.Fatalf("missing parameter was not reported: %v", err)
		}
	}
	if _, err := transform.Transform(ctx, nil, "url"); err == nil {
		t.Fatal("nil resolver lookup accepted")
	}
}
