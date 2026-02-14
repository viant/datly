package xgen

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/typectx"
)

func TestGenerateFromDQLShape(t *testing.T) {
	projectDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte("module example.com/demo\n\ngo 1.25.0\n"), 0o644); err != nil {
		t.Fatalf("write go.mod failed: %v", err)
	}
	doc := &dqlshape.Document{Root: map[string]any{
		"Routes": []any{
			map[string]any{
				"Name": "orders",
				"URI":  "/orders",
				"View": map[string]any{"Ref": "orders"},
				"Input": map[string]any{
					"Type": map[string]any{"Name": "OrdersFilter"},
					"Parameters": []any{
						map[string]any{
							"Name": "status",
							"Schema": map[string]any{
								"DataType": "string",
							},
						},
					},
				},
				"Output": map[string]any{
					"Type": map[string]any{"Name": "OrdersPayload"},
					"Parameters": []any{
						map[string]any{
							"Name": "total",
							"Schema": map[string]any{
								"DataType": "int",
							},
						},
					},
				},
			},
		},
		"Resource": map[string]any{
			"Views": []any{
				map[string]any{
					"Name": "orders",
					"Schema": map[string]any{
						"Name": "OrderView",
					},
					"ColumnsConfig": map[string]any{
						"ID":   map[string]any{"Name": "ID", "DataType": "int"},
						"NAME": map[string]any{"Name": "NAME", "DataType": "string"},
					},
				},
			},
		},
	}}
	result, err := GenerateFromDQLShape(doc, &Config{
		ProjectDir:  projectDir,
		PackageDir:  "internal/gen",
		PackageName: "gen",
		FileName:    "shapes_gen.go",
		TypePrefix:  "DQL",
	})
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}
	if result == nil {
		t.Fatalf("nil result")
	}
	if len(result.Types) == 0 {
		t.Fatalf("expected generated types")
	}
	if _, err = os.Stat(result.FilePath); err != nil {
		t.Fatalf("generated file missing: %v", err)
	}
	data, err := os.ReadFile(result.FilePath)
	if err != nil {
		t.Fatalf("read generated file failed: %v", err)
	}
	source := string(data)
	if !strings.Contains(source, "type DQLOrderView struct") {
		t.Fatalf("expected generated type in source, got:\n%s", source)
	}
	if !strings.Contains(source, "type DQLOrdersFilterInput struct") || !strings.Contains(source, "type DQLOrdersPayloadOutput struct") {
		t.Fatalf("expected io types in source, got:\n%s", source)
	}
	if !strings.Contains(source, "Id") || !strings.Contains(source, "Name") {
		t.Fatalf("expected generated fields in source, got:\n%s", source)
	}
	fset := token.NewFileSet()
	if _, err = parser.ParseFile(fset, result.FilePath, source, parser.AllErrors); err != nil {
		t.Fatalf("generated file parse failed: %v", err)
	}
}

func TestGenerateFromDQLShape_CustomTypeNamers(t *testing.T) {
	projectDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte("module example.com/demo\n\ngo 1.25.0\n"), 0o644); err != nil {
		t.Fatalf("write go.mod failed: %v", err)
	}
	doc := &dqlshape.Document{Root: map[string]any{
		"Routes": []any{
			map[string]any{
				"Name": "orders",
				"Input": map[string]any{
					"Parameters": []any{map[string]any{"Name": "q", "Schema": map[string]any{"DataType": "string"}}},
				},
				"Output": map[string]any{
					"Parameters": []any{map[string]any{"Name": "count", "Schema": map[string]any{"DataType": "int"}}},
				},
			},
		},
		"Resource": map[string]any{
			"Views": []any{
				map[string]any{"Name": "orders", "ColumnsConfig": map[string]any{"ID": map[string]any{"Name": "ID", "DataType": "int"}}},
			},
		},
	}}
	result, err := GenerateFromDQLShape(doc, &Config{
		ProjectDir: projectDir,
		PackageDir: "internal/gen",
		ViewTypeNamer: func(ctx ViewTypeContext) string {
			return "DataOrders"
		},
		InputTypeNamer: func(ctx RouteTypeContext) string {
			return "ReqOrders"
		},
		OutputTypeNamer: func(ctx RouteTypeContext) string {
			return "ResOrders"
		},
	})
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}
	data, err := os.ReadFile(result.FilePath)
	if err != nil {
		t.Fatalf("read generated file failed: %v", err)
	}
	source := string(data)
	if !strings.Contains(source, "type DataOrders struct") {
		t.Fatalf("missing custom view type: %s", source)
	}
	if !strings.Contains(source, "type ReqOrders struct") {
		t.Fatalf("missing custom input type: %s", source)
	}
	if !strings.Contains(source, "type ResOrders struct") {
		t.Fatalf("missing custom output type: %s", source)
	}
}

func TestGenerateFromDQLShape_BlocksUnsafeRewriteByProvenance(t *testing.T) {
	projectDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte("module example.com/demo\n\ngo 1.25.0\n"), 0o644); err != nil {
		t.Fatalf("write go.mod failed: %v", err)
	}
	packageDir := filepath.Join(projectDir, "internal", "gen")
	if err := os.MkdirAll(packageDir, 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	dest := filepath.Join(packageDir, "shapes_gen.go")
	if err := os.WriteFile(dest, []byte("package gen\n"), 0o644); err != nil {
		t.Fatalf("seed file failed: %v", err)
	}

	doc := &dqlshape.Document{
		Root: map[string]any{
			"Resource": map[string]any{
				"Views": []any{
					map[string]any{"Name": "orders", "ColumnsConfig": map[string]any{"ID": map[string]any{"Name": "ID", "DataType": "int"}}},
				},
			},
		},
		TypeResolutions: []typectx.Resolution{
			{
				Expression: "Fee",
				Provenance: typectx.Provenance{Kind: "registry"},
			},
		},
	}
	_, err := GenerateFromDQLShape(doc, &Config{
		ProjectDir:  projectDir,
		PackageDir:  "internal/gen",
		PackageName: "gen",
		FileName:    "shapes_gen.go",
	})
	if err == nil || !strings.Contains(err.Error(), "rewrite blocked") {
		t.Fatalf("expected rewrite blocked error, got: %v", err)
	}
}

func TestGenerateFromDQLShape_AllowsUnsafeRewriteWithOverride(t *testing.T) {
	projectDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte("module example.com/demo\n\ngo 1.25.0\n"), 0o644); err != nil {
		t.Fatalf("write go.mod failed: %v", err)
	}
	packageDir := filepath.Join(projectDir, "internal", "gen")
	if err := os.MkdirAll(packageDir, 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	dest := filepath.Join(packageDir, "shapes_gen.go")
	if err := os.WriteFile(dest, []byte("package gen\n"), 0o644); err != nil {
		t.Fatalf("seed file failed: %v", err)
	}

	doc := &dqlshape.Document{
		Root: map[string]any{
			"Resource": map[string]any{
				"Views": []any{
					map[string]any{"Name": "orders", "ColumnsConfig": map[string]any{"ID": map[string]any{"Name": "ID", "DataType": "int"}}},
				},
			},
		},
		TypeResolutions: []typectx.Resolution{
			{
				Expression: "Fee",
				Provenance: typectx.Provenance{Kind: "registry"},
			},
		},
	}
	result, err := GenerateFromDQLShape(doc, &Config{
		ProjectDir:         projectDir,
		PackageDir:         "internal/gen",
		PackageName:        "gen",
		FileName:           "shapes_gen.go",
		AllowUnsafeRewrite: true,
	})
	if err != nil {
		t.Fatalf("expected override rewrite success, got: %v", err)
	}
	if result == nil || result.FilePath == "" {
		t.Fatalf("expected generated result")
	}
}

func TestGenerateFromDQLShape_MergesIntoExistingFile(t *testing.T) {
	projectDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte("module example.com/demo\n\ngo 1.25.0\n"), 0o644); err != nil {
		t.Fatalf("write go.mod failed: %v", err)
	}
	packageDir := filepath.Join(projectDir, "internal", "gen")
	if err := os.MkdirAll(packageDir, 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	dest := filepath.Join(packageDir, "shapes_gen.go")
	initial := `package gen

type DQLOrderView struct {
	Old string ` + "`json:\"old,omitempty\"`" + `
}

func KeepCustom() string { return "ok" }
`
	if err := os.WriteFile(dest, []byte(initial), 0o644); err != nil {
		t.Fatalf("seed file failed: %v", err)
	}

	doc := &dqlshape.Document{Root: map[string]any{
		"Resource": map[string]any{
			"Views": []any{
				map[string]any{
					"Name": "orders",
					"Schema": map[string]any{
						"Name": "OrderView",
					},
					"ColumnsConfig": map[string]any{
						"ID": map[string]any{"Name": "ID", "DataType": "int"},
					},
				},
			},
		},
	}}
	_, err := GenerateFromDQLShape(doc, &Config{
		ProjectDir:  projectDir,
		PackageDir:  "internal/gen",
		PackageName: "gen",
		FileName:    "shapes_gen.go",
		TypePrefix:  "DQL",
	})
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}

	data, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read generated file failed: %v", err)
	}
	source := string(data)
	if !strings.Contains(source, "func KeepCustom() string") {
		t.Fatalf("expected custom function preserved, got:\n%s", source)
	}
	if strings.Contains(source, "Old string") {
		t.Fatalf("expected old shape declaration replaced, got:\n%s", source)
	}
	if !strings.Contains(source, "type DQLOrderView struct") || !strings.Contains(source, "Id int") {
		t.Fatalf("expected updated shape declaration, got:\n%s", source)
	}
}
