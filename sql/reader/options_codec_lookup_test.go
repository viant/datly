package reader

import (
	"context"
	"errors"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

func TestReaderCodecLookupPreservesOptionality(t *testing.T) {
	yes, no := true, false
	session := &Session{Component: &spec.Component{Parameters: []*spec.Parameter{
		{Name: "Optional", Required: &no}, {Name: "Required", Required: &yes},
	}}, Parameters: func(string) (any, bool, error) { return nil, false, nil }}
	options := newReaderOptions(session, &data.View{}, nil)
	value, err := options.codecValue(context.Background(), "Optional")
	if err != nil || value != nil {
		t.Fatalf("optional = %v, %v", value, err)
	}
	for _, name := range []string{"Required", "Unknown"} {
		if _, err := options.codecValue(context.Background(), name); err == nil {
			t.Fatalf("missing %s accepted", name)
		}
	}
	cause := errors.New("parameter conversion failed")
	options.parameters = func(string) (any, bool, error) { return nil, false, cause }
	if _, err := options.codecValue(context.Background(), "Optional"); !errors.Is(err, cause) {
		t.Fatalf("resolver error lost: %v", err)
	}
	options.parameters = nil
	if _, err := options.codecValue(context.Background(), "Optional"); err != nil {
		t.Fatalf("absent optional resolver: %v", err)
	}
	if _, err := options.codecValue(context.Background(), "Required"); err == nil {
		t.Fatal("required resolver failure lost")
	}
	for _, input := range []any{0, false, ""} {
		options.parameters = func(string) (any, bool, error) { return input, true, nil }
		value, err := options.codecValue(context.Background(), "Optional")
		if err != nil || value != input {
			t.Fatalf("explicit value = %v, %v", value, err)
		}
	}
}
