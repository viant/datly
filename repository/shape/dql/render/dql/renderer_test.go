package dql

import (
	"errors"
	"testing"

	"github.com/viant/datly/repository/shape/dql/ir"
)

func TestEncode_WithEmbeddedSource(t *testing.T) {
	doc := &ir.Document{Root: map[string]any{
		"Routes": []any{
			map[string]any{
				"View": map[string]any{"Ref": "root"},
			},
		},
		"Resource": map[string]any{
			"Views": []any{
				map[string]any{
					"Name": "root",
					"Template": map[string]any{
						"Source": "SELECT * FROM USERS u",
					},
				},
			},
		},
	}}
	data, err := Encode(doc)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	if got, want := string(data), "SELECT * FROM USERS u\n"; got != want {
		t.Fatalf("unexpected dql, got %q want %q", got, want)
	}
}

func TestEncode_WithSourceURLResolver(t *testing.T) {
	doc := &ir.Document{Root: map[string]any{
		"Routes": []any{
			map[string]any{
				"View": map[string]any{"Ref": "root"},
			},
		},
		"Resource": map[string]any{
			"Views": []any{
				map[string]any{
					"Name": "root",
					"Template": map[string]any{
						"SourceURL": "queries/root.sql",
					},
				},
			},
		},
	}}
	data, err := Encode(doc, WithSourceResolver(func(sourceURL string) (string, error) {
		if sourceURL != "queries/root.sql" {
			t.Fatalf("unexpected sourceURL: %s", sourceURL)
		}
		return "SELECT 1", nil
	}))
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	if got, want := string(data), "SELECT 1\n"; got != want {
		t.Fatalf("unexpected dql, got %q want %q", got, want)
	}
}

func TestEncode_SourceURLWithoutResolverFails(t *testing.T) {
	doc := &ir.Document{Root: map[string]any{
		"Routes": []any{
			map[string]any{
				"View": map[string]any{"Ref": "root"},
			},
		},
		"Resource": map[string]any{
			"Views": []any{
				map[string]any{
					"Name": "root",
					"Template": map[string]any{
						"SourceURL": "queries/root.sql",
					},
				},
			},
		},
	}}
	_, err := Encode(doc)
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestEncode_ResolverError(t *testing.T) {
	doc := &ir.Document{Root: map[string]any{
		"Routes": []any{
			map[string]any{
				"View": map[string]any{"Ref": "root"},
			},
		},
		"Resource": map[string]any{
			"Views": []any{
				map[string]any{
					"Name": "root",
					"Template": map[string]any{
						"SourceURL": "queries/root.sql",
					},
				},
			},
		},
	}}
	_, err := Encode(doc, WithSourceResolver(func(sourceURL string) (string, error) {
		return "", errors.New("boom")
	}))
	if err == nil {
		t.Fatalf("expected error")
	}
}
