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

func TestGenerateFromDQLShape_UsesTypeContextPackageDefaults(t *testing.T) {
	projectDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte("module example.com/demo\n\ngo 1.25.0\n"), 0o644); err != nil {
		t.Fatalf("write go.mod failed: %v", err)
	}
	doc := &dqlshape.Document{
		TypeContext: &typectx.Context{
			PackageDir:  "pkg/platform/taxonomy",
			PackageName: "taxonomy",
			PackagePath: "example.com/demo/pkg/platform/taxonomy",
		},
		Root: map[string]any{
			"Resource": map[string]any{
				"Views": []any{
					map[string]any{
						"Name": "orders",
						"ColumnsConfig": map[string]any{
							"ID": map[string]any{"Name": "ID", "DataType": "int"},
						},
					},
				},
			},
		},
	}
	result, err := GenerateFromDQLShape(doc, &Config{ProjectDir: projectDir})
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}
	if result == nil {
		t.Fatalf("expected result")
	}
	if result.PackageName != "taxonomy" {
		t.Fatalf("expected package name taxonomy, got %q", result.PackageName)
	}
	if result.PackagePath != "example.com/demo/pkg/platform/taxonomy" {
		t.Fatalf("expected package path from type context, got %q", result.PackagePath)
	}
	if !strings.Contains(filepath.ToSlash(result.FilePath), "/pkg/platform/taxonomy/") {
		t.Fatalf("expected file under type-context package dir, got %s", result.FilePath)
	}
}

func TestGenerateFromDQLShape_ProvenanceEnrichment_WithReplaceAndTypeContextPackagePath(t *testing.T) {
	root := t.TempDir()
	projectDir := filepath.Join(root, "project")
	modelsDir := filepath.Join(root, "shared-models")
	if err := os.MkdirAll(filepath.Join(projectDir, "internal", "gen"), 0o755); err != nil {
		t.Fatalf("mkdir project failed: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(modelsDir, "mdp"), 0o755); err != nil {
		t.Fatalf("mkdir models failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte("module example.com/project\n\ngo 1.25\nreplace github.com/acme/models => ../shared-models\n"), 0o644); err != nil {
		t.Fatalf("write project go.mod failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(modelsDir, "go.mod"), []byte("module github.com/acme/models\n\ngo 1.25\n"), 0o644); err != nil {
		t.Fatalf("write models go.mod failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(modelsDir, "mdp", "types.go"), []byte("package mdp\ntype Order struct{}\n"), 0o644); err != nil {
		t.Fatalf("write types.go failed: %v", err)
	}
	dest := filepath.Join(projectDir, "internal", "gen", "shapes_gen.go")
	if err := os.WriteFile(dest, []byte("package gen\n"), 0o644); err != nil {
		t.Fatalf("seed file failed: %v", err)
	}

	doc := &dqlshape.Document{
		TypeContext: &typectx.Context{
			PackagePath: "github.com/acme/models/mdp",
		},
		Root: map[string]any{
			"Resource": map[string]any{
				"Views": []any{
					map[string]any{
						"Name": "orders",
						"ColumnsConfig": map[string]any{
							"ID": map[string]any{"Name": "ID", "DataType": "int"},
						},
					},
				},
			},
		},
		TypeResolutions: []typectx.Resolution{
			{
				Expression: "Order",
				ResolvedKey: "Order",
				Provenance: typectx.Provenance{
					Kind: "registry",
				},
			},
		},
	}

	_, err := GenerateFromDQLShape(doc, &Config{
		ProjectDir:         projectDir,
		PackageDir:         "internal/gen",
		PackageName:        "gen",
		FileName:           "shapes_gen.go",
		AllowedSourceRoots: []string{modelsDir},
	})
	if err != nil {
		t.Fatalf("expected provenance enrichment to allow rewrite, got: %v", err)
	}
}

func TestGenerateFromDQLShape_ProvenanceEnrichment_WithGOPATHFallback(t *testing.T) {
	root := t.TempDir()
	projectDir := filepath.Join(root, "project")
	gopath := filepath.Join(root, "gopath")
	modelsDir := filepath.Join(gopath, "src", "github.com", "legacy", "models")
	if err := os.MkdirAll(filepath.Join(projectDir, "internal", "gen"), 0o755); err != nil {
		t.Fatalf("mkdir project failed: %v", err)
	}
	if err := os.MkdirAll(modelsDir, 0o755); err != nil {
		t.Fatalf("mkdir models failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte("module example.com/project\n\ngo 1.25\n"), 0o644); err != nil {
		t.Fatalf("write project go.mod failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(modelsDir, "types.go"), []byte("package models\ntype Legacy struct{}\n"), 0o644); err != nil {
		t.Fatalf("write types.go failed: %v", err)
	}
	dest := filepath.Join(projectDir, "internal", "gen", "shapes_gen.go")
	if err := os.WriteFile(dest, []byte("package gen\n"), 0o644); err != nil {
		t.Fatalf("seed file failed: %v", err)
	}

	orig := os.Getenv("GOPATH")
	if err := os.Setenv("GOPATH", gopath); err != nil {
		t.Fatalf("set GOPATH failed: %v", err)
	}
	defer func() { _ = os.Setenv("GOPATH", orig) }()

	doc := &dqlshape.Document{
		TypeContext: &typectx.Context{
			PackagePath: "github.com/legacy/models",
		},
		Root: map[string]any{
			"Resource": map[string]any{
				"Views": []any{
					map[string]any{
						"Name": "legacy",
						"ColumnsConfig": map[string]any{
							"ID": map[string]any{"Name": "ID", "DataType": "int"},
						},
					},
				},
			},
		},
		TypeResolutions: []typectx.Resolution{
			{
				Expression:  "Legacy",
				ResolvedKey: "Legacy",
				Provenance:  typectx.Provenance{Kind: "registry"},
			},
		},
	}
	_, err := GenerateFromDQLShape(doc, &Config{
		ProjectDir:         projectDir,
		PackageDir:         "internal/gen",
		PackageName:        "gen",
		FileName:           "shapes_gen.go",
		AllowedSourceRoots: []string{filepath.Join(gopath, "src")},
		UseGoModuleResolve: boolPtr(false),
		UseGOPATHFallback:  boolPtr(true),
	})
	if err != nil {
		t.Fatalf("expected GOPATH provenance enrichment to allow rewrite, got: %v", err)
	}
}

func boolPtr(value bool) *bool {
	return &value
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
