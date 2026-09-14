package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSkillpackExactImportsAndHostileReferences(t *testing.T) {
	source, products, out := t.TempDir(), t.TempDir(), filepath.Join(t.TempDir(), "installed")
	write := func(root, name, text string) {
		t.Helper()
		target := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, []byte(text), 0644); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"datly-reader", "datly-writer", "datly-custom-component"} {
		write(source, name+"/SKILL.md", "---\nname: "+name+"\ndescription: Example.\n---\n[guide](references/product/datly/doc/guide.md#guide)")
	}
	write(products, "datly/doc/guide.md", "# Guide\n\n[details](details.md#details)\n\n```md\n[untouched](private.md)\n```\n")
	write(products, "datly/doc/details.md", "# Details\n\nAuthoritative reference.")
	write(products, "datly/doc/private.md", "PRIVATE")
	declaration := productDeclaration{Files: []string{"datly/doc/guide.md", "datly/doc/details.md"}}
	raw, _ := json.Marshal(declaration)
	write(source, "packaging.json", string(raw))
	p := packager{source: source, productsRoot: products, destination: out, write: true}
	if err := p.run(); err != nil {
		t.Fatal(err)
	}
	p.write = false
	if err := p.run(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(out, "datly-reader/references/product/datly/doc/guide.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte("](references/product/datly/doc/details.md#details)")) || !bytes.Contains(data, []byte("[untouched](private.md)")) {
		t.Fatal("parser-bound transformation failed")
	}
	if _, err = os.Stat(filepath.Join(out, "datly-reader/references/product/datly/doc/private.md")); !os.IsNotExist(err) {
		t.Fatal("unapproved sibling copied")
	}
	write(products, "datly/doc/guide.md", "# Guide\n[private](private.md)")
	if err = p.run(); err == nil || !strings.Contains(err.Error(), "undeclared product references") {
		t.Fatalf("implicit import: %v", err)
	}
	write(products, "datly/doc/guide.md", "# Guide\n[bad](details.md#missing)")
	if err = p.run(); err == nil || !strings.Contains(err.Error(), "unresolved heading") {
		t.Fatalf("bad heading: %v", err)
	}
	if err = os.Remove(filepath.Join(products, "datly/doc/details.md")); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink("private.md", filepath.Join(products, "datly/doc/details.md")); err != nil {
		t.Fatal(err)
	}
	if err = p.run(); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("symlink: %v", err)
	}
}

func TestSkillpackReferenceStyleCannotSelectLaterCode(t *testing.T) {
	source := []byte("[guide][ref]\n\n[ref]: guide.md\n\n```md\n[example](guide.md)\n```\n")
	if _, err := rewriteLinks(source, func(string) (string, error) { return "modified.md", nil }); err == nil {
		t.Fatal("reference-style link selected a later code-example destination")
	}
}

func TestSkillpackPublicExportBoundaries(t *testing.T) {
	source, products := t.TempDir(), t.TempDir()
	if err := os.Mkdir(filepath.Join(products, "datly"), 0755); err != nil {
		t.Fatal(err)
	}
	guide := "[![badge](https://example.com/badge.svg)](https://example.com)\n# Product\nPublic guide.\n# Contributors\n[private](private.md)\n"
	if err := os.WriteFile(filepath.Join(products, "datly", "README.md"), []byte(guide), 0644); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		start, end string
		valid      bool
	}{
		{"# Product", "# Contributors", true},
		{"# Missing", "# Contributors", false},
		{"# Product", "# Missing", false},
		{"# Contributors", "# Product", false},
	} {
		t.Run(tc.start+"/"+tc.end, func(t *testing.T) {
			declaration := productDeclaration{
				Files: []string{"datly/README.md"},
				Exports: map[string]struct{ StartAt, EndBefore, Reason string }{
					"datly/README.md": {StartAt: tc.start, EndBefore: tc.end, Reason: "Public guide only."},
				},
			}
			raw, err := json.Marshal(declaration)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(source, "packaging.json"), raw, 0644); err != nil {
				t.Fatal(err)
			}
			contents := map[string][]byte{}
			provenance, err := (packager{source: source, productsRoot: products}).products(contents)
			if !tc.valid {
				if err == nil {
					t.Fatal("invalid export boundaries accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, data := range contents {
				if !bytes.Contains(data, []byte("Public guide.")) || bytes.Contains(data, []byte("badge")) || bytes.Contains(data, []byte("private.md")) {
					t.Fatalf("incorrect export: %s", data)
				}
			}
			if len(provenance["datly/README.md"]) != 64 {
				t.Fatal("source provenance missing")
			}
		})
	}
}

func TestSkillpackDistributedSourcesUseSelectedRoot(t *testing.T) {
	source, products, out := t.TempDir(), t.TempDir(), filepath.Join(t.TempDir(), "bundle")
	for _, skill := range []string{"datly-reader", "datly-writer", "datly-custom-component"} {
		if err := os.MkdirAll(filepath.Join(source, skill), 0755); err != nil {
			t.Fatal(err)
		}
		data := "---\nname: " + skill + "\ndescription: Example.\n---\n# Canonical source\n[guide](references/product/datly/doc/guide.md)\n"
		if err := os.WriteFile(filepath.Join(source, skill, "SKILL.md"), []byte(data), 0644); err != nil {
			t.Fatal(err)
		}
	}
	for name, data := range map[string]string{
		"datly/doc/guide.md":              "# Guide\n[reader](../llm/datly-reader/SKILL.md)\n",
		"datly/llm/datly-reader/SKILL.md": "SHADOW SOURCE MUST NOT BE READ",
	} {
		filename := filepath.Join(products, name)
		if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filename, []byte(data), 0644); err != nil {
			t.Fatal(err)
		}
	}
	declaration, err := json.Marshal(productDeclaration{Files: []string{"datly/doc/guide.md", "llm/datly-reader/SKILL.md"}})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(source, "packaging.json"), declaration, 0644); err != nil {
		t.Fatal(err)
	}
	p := packager{source: source, productsRoot: products, destination: out, write: true}
	if err = p.run(); err != nil {
		t.Fatal(err)
	}
	p.write = false
	if err = p.run(); err != nil {
		t.Fatal(err)
	}
	guide, err := os.ReadFile(filepath.Join(out, "datly-reader/references/product/datly/doc/guide.md"))
	if err != nil || !bytes.Contains(guide, []byte("](SKILL.md)")) {
		t.Fatalf("distributed link: %s %v", guide, err)
	}
	imported, err := os.ReadFile(filepath.Join(out, "datly-writer/references/product/llm/datly-reader/SKILL.reference.md"))
	if err != nil || !bytes.Contains(imported, []byte("# Canonical source")) || bytes.Contains(imported, []byte("SHADOW")) {
		t.Fatalf("source authority: %s %v", imported, err)
	}
}
