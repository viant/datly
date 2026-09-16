package standalone

import (
	"context"
	"errors"
	"reflect"
	"testing"

	xcodec "github.com/viant/xdatly/codec"
)

type factoryFunc func(*xcodec.Config, ...xcodec.Option) (xcodec.Instance, error)

func (f factoryFunc) New(c *xcodec.Config, o ...xcodec.Option) (xcodec.Instance, error) {
	return f(c, o...)
}

type echoCodec struct{}

func (*echoCodec) Value(_ context.Context, v any, _ ...xcodec.Option) (any, error) { return v, nil }

func TestApplicationCodecs(t *testing.T) {
	factory := factoryFunc(func(c *xcodec.Config, o ...xcodec.Option) (xcodec.Instance, error) {
		if c.SourceType != reflect.TypeFor[string]() || c.DestinationType != reflect.TypeFor[string]() {
			t.Fatal("lost types")
		}
		if xcodec.NewOptions(o).Record != "record" {
			t.Fatal("lost options")
		}
		c.Args[0] = "mutated"
		return &echoCodec{}, nil
	})
	named := map[string]xcodec.Factory{" Echo ": factory}
	normalized, err := normalizeCodecs(named)
	if err != nil {
		t.Fatal(err)
	}
	delete(named, " Echo ")
	fallbackError := errors.New("fallback")
	composed := &applicationCodecs{factories: normalized, fallback: factoryFunc(func(c *xcodec.Config, o ...xcodec.Option) (xcodec.Instance, error) {
		if c.Body != "JwtClaim" {
			t.Fatal("wrong fallback")
		}
		return nil, fallbackError
	})}
	cfg := &xcodec.Config{Body: "ECHO", SourceType: reflect.TypeFor[string](), DestinationType: reflect.TypeFor[string](), Args: []string{"original"}}
	if _, err = composed.New(cfg, xcodec.WithRecord("record")); err != nil {
		t.Fatal(err)
	}
	if cfg.Args[0] != "original" {
		t.Fatal("caller config mutated")
	}
	if _, err = composed.New(&xcodec.Config{Body: "JwtClaim"}); !errors.Is(err, fallbackError) {
		t.Fatal("fallback error not preserved", err)
	}
	if _, err = composed.New(nil); err == nil {
		t.Fatal("nil config accepted")
	}
	if _, err = (&applicationCodecs{}).New(&xcodec.Config{Body: "unknown"}); err == nil {
		t.Fatal("unknown codec accepted")
	}
}

func TestApplicationCodecValidation(t *testing.T) {
	f := factoryFunc(func(*xcodec.Config, ...xcodec.Option) (xcodec.Instance, error) { return &echoCodec{}, nil })
	var nilFactory factoryFunc
	for _, input := range []map[string]xcodec.Factory{
		{"": f}, {" JwtClaim ": f}, {"jwtclaims": f}, {"STRUCTQL": f}, {"foo": nil}, {"foo": nilFactory}, {"foo": f, " FOO ": f},
	} {
		if _, err := normalizeCodecs(input); err == nil {
			t.Fatalf("invalid registration accepted: %v", input)
		}
	}
}
