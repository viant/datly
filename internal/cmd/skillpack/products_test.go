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
	if !bytes.Contains(data, []byte("](details.md#details)")) || !bytes.Contains(data, []byte("[untouched](private.md)")) {
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
	if err != nil || !bytes.Contains(guide, []byte("](../../../../SKILL.md)")) {
		t.Fatalf("distributed link: %s %v", guide, err)
	}
	imported, err := os.ReadFile(filepath.Join(out, "datly-writer/references/product/llm/datly-reader/SKILL.reference.md"))
	if err != nil || !bytes.Contains(imported, []byte("# Canonical source")) || bytes.Contains(imported, []byte("SHADOW")) {
		t.Fatalf("source authority: %s %v", imported, err)
	}
}

// Check the published files using normal filesystem resolution, independently
// of the packager's intermediate skill-relative target representation.
func TestSkillpackNestedLinksResolveToCanonicalContract(t *testing.T) {
	source, products, out := t.TempDir(), t.TempDir(), filepath.Join(t.TempDir(), "installed")
	write := func(root, name, data string) {
		t.Helper()
		filename := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filename, []byte(data), 0644); err != nil {
			t.Fatal(err)
		}
	}
	for _, skill := range []string{"datly-reader", "datly-writer", "datly-custom-component"} {
		write(source, skill+"/SKILL.md", "---\nname: "+skill+"\ndescription: Example.\n---\n[guide](references/guide.md)\n")
		link := "examples.md#complete"
		if skill == "datly-writer" {
			link = "references/examples.md#complete"
		}
		write(source, skill+"/references/guide.md", "# Guide\n[contract]("+link+")\n")
		write(source, skill+"/references/examples.md", "# Complete\nCanonical "+skill+" contract.\n")
	}
	write(products, "datly/doc/guide.md", "# Product\n[contract](../../llm/datly-reader/references/examples.md#complete)\n")
	declaration := productDeclaration{Files: []string{"datly/doc/guide.md", "llm/datly-reader/references/guide.md", "llm/datly-reader/references/examples.md", "llm/datly-writer/references/guide.md", "llm/datly-writer/references/examples.md"}, SkillRootLinks: []string{"datly-writer"}}
	raw, err := json.Marshal(declaration)
	if err != nil {
		t.Fatal(err)
	}
	write(source, "packaging.json", string(raw))
	originalWriter, err := os.ReadFile(filepath.Join(source, "datly-writer/references/guide.md"))
	if err != nil {
		t.Fatal(err)
	}
	if err = (packager{source: source, productsRoot: products, destination: out, write: true}).run(); err != nil {
		t.Fatal(err)
	}
	for _, skill := range []string{"datly-reader", "datly-writer", "datly-custom-component"} {
		for _, tc := range []struct{ from, target string }{
			{"references/guide.md", "references/examples.md"},
			{"references/product/datly/doc/guide.md", "references/product/llm/datly-reader/references/examples.md"},
			{"references/product/llm/datly-reader/references/guide.md", "references/product/llm/datly-reader/references/examples.md"},
			{"references/product/llm/datly-writer/references/guide.md", "references/product/llm/datly-writer/references/examples.md"},
		} {
			target := tc.target
			if skill == "datly-reader" && strings.Contains(tc.from, "/product/") && !strings.Contains(tc.from, "/datly-writer/") {
				target = "references/examples.md"
			}
			if skill == "datly-writer" && strings.Contains(tc.from, "/product/llm/datly-writer/") {
				target = "references/examples.md"
			}
			filename := filepath.Join(out, skill, tc.from)
			data, err := os.ReadFile(filename)
			if err != nil {
				t.Fatal(err)
			}
			links, _, err := markdownLinks(data)
			if err != nil || len(links) != 1 {
				t.Fatalf("links %s: %v %v", filename, links, err)
			}
			u, err := localReference(links[0].target)
			if err != nil || u == nil {
				t.Fatalf("local target: %v", err)
			}
			resolved := filepath.Clean(filepath.Join(filepath.Dir(filename), filepath.FromSlash(u.Path)))
			want := filepath.Join(out, skill, target)
			if resolved != want {
				t.Fatalf("%s -> %s resolves %s, want canonical file %s", filename, links[0].target, resolved, want)
			}
			body, err := os.ReadFile(resolved)
			if err != nil || !bytes.Contains(body, []byte("# Complete\nCanonical ")) {
				t.Fatalf("wrong actual contract: %s: %v", resolved, err)
			}
			if u.Fragment != "complete" {
				t.Fatalf("fragment lost: %s", links[0].target)
			}
		}
	}
	writer, err := os.ReadFile(filepath.Join(source, "datly-writer/references/guide.md"))
	if err != nil || !bytes.Equal(writer, originalWriter) {
		t.Fatal("canonical writer input modified")
	}
}

func TestSkillpackRejectsRootRelativeFalsePositive(t *testing.T) {
	contents := map[string][]byte{
		"datly-reader/references/guide.md":    []byte("[contract](references/examples.md#complete)"),
		"datly-reader/references/examples.md": []byte("# Complete"),
	}
	if err := validateLinks(contents); err == nil || !strings.Contains(err.Error(), "references/references/examples.md") {
		t.Fatalf("wrong directory accepted: %v", err)
	}
	contents["datly-reader/references/guide.md"] = []byte("[contract](examples.md#complete)")
	if err := validateLinks(contents); err != nil {
		t.Fatal(err)
	}
	contents["datly-reader/references/guide.md"] = []byte("[contract](../../outside.md)")
	if err := validateLinks(contents); err == nil || !strings.Contains(err.Error(), "escapes skill root") {
		t.Fatalf("escape accepted: %v", err)
	}
}
