package expand_test

import (
	"testing"

	"github.com/viant/datly/service/executor/expand"
)

func TestNewEvaluator_DefaultTypeLookup(t *testing.T) {
	evaluator, err := expand.NewEvaluator(`#set($x = $New("int"))$x`)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if _, err := evaluator.Evaluate(nil); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestNewEvaluator_WithNilTypeLookupOption(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("expected no panic, got %v", r)
		}
	}()

	evaluator, err := expand.NewEvaluator(`#set($x = $New("int"))$x`, expand.WithTypeLookup(nil))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if _, err := evaluator.Evaluate(nil); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestNewEvaluator_UnknownTypeReturnsError(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("expected no panic, got %v", r)
		}
	}()

	_, err := expand.NewEvaluator(`#set($x = $New("DefinitelyNotAType"))$x`, expand.WithTypeLookup(nil))
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestNewEvaluator_WithNilNamedVariableOption(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("expected no panic, got %v", r)
		}
	}()

	evaluator, err := expand.NewEvaluator(`ok`, expand.WithVariable(nil))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if _, err := evaluator.Evaluate(nil); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestNewEvaluator_WithNilCustomContextOption(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("expected no panic, got %v", r)
		}
	}()

	evaluator, err := expand.NewEvaluator(`ok`, expand.WithCustomContexts(nil))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if _, err := evaluator.Evaluate(nil); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}
