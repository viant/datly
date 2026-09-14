// Package main implements skillpack, which reproducibly packages canonical skills for embedding.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	skillformat "github.com/viant/mcp-protocol/extension/skills"
	"io/fs"
	"os"
	"path/filepath"
)

type packager struct {
	source, destination string
	productsRoot        string
	write               bool
}

func main() {
	p := packager{}
	flag.StringVar(&p.source, "source", "llm", "canonical skill root distributed with Datly")
	flag.StringVar(&p.destination, "out", "mcp/developer/skills/assets", "generated embedded tree")
	flag.BoolVar(&p.write, "write", false, "synchronize instead of checking")
	flag.StringVar(&p.productsRoot, "products", "..", "authoritative product workspace for exact declared imports")
	flag.Parse()
	if err := p.run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func (p packager) run() error {
	contents := map[string][]byte{}
	hashes := map[string]string{}
	for _, folder := range []string{"datly-reader", "datly-writer", "datly-custom-component"} {
		root := os.DirFS(p.source)
		if err := fs.WalkDir(root, folder, func(name string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.Type()&fs.ModeSymlink != 0 {
				return fmt.Errorf("canonical skill contains symlink: %s", name)
			}
			if entry.IsDir() {
				return nil
			}
			data, err := fs.ReadFile(root, name)
			if err != nil {
				return err
			}
			contents[name] = data
			hashes[name] = fmt.Sprintf("%x", sha256.Sum256(data))
			return nil
		}); err != nil {
			return err
		}
	}
	provenance, err := p.products(contents)
	if err != nil {
		return err
	}
	if err = validateLinks(contents); err != nil {
		return err
	}
	for name, data := range contents {
		hashes[name] = fmt.Sprintf("%x", sha256.Sum256(data))
	}
	manifest, err := json.MarshalIndent(struct {
		Source               string
		Files                map[string]string
		ProductSources       map[string]string
		UnresolvedReferences []string
	}{Source: "canonical skill sources plus exact packaging.json product imports", Files: hashes, ProductSources: provenance, UnresolvedReferences: []string{}}, "", "  ")
	if err != nil {
		return err
	}
	contents["manifest.json"] = append(manifest, '\n')
	if !p.write {
		seen := map[string]bool{}
		err := filepath.WalkDir(p.destination, func(location string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			name, err := filepath.Rel(p.destination, location)
			if err != nil {
				return err
			}
			name = filepath.ToSlash(name)
			data, err := os.ReadFile(location)
			if err != nil {
				return err
			}
			if !bytes.Equal(data, contents[name]) {
				return fmt.Errorf("embedded skill drift: %s", name)
			}
			seen[name] = true
			return nil
		})
		if err != nil {
			return err
		}
		if len(seen) != len(contents) {
			return fmt.Errorf("embedded skill files missing")
		}
		return p.validateSkills(p.destination)
	}
	parent := filepath.Dir(p.destination)
	if err := os.MkdirAll(parent, 0755); err != nil {
		return err
	}
	stage, err := os.MkdirTemp(parent, ".skills-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	for name, data := range contents {
		destination := filepath.Join(stage, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(destination), 0755); err != nil {
			return err
		}
		if err := os.WriteFile(destination, data, 0644); err != nil {
			return err
		}
	}
	if err := p.validateSkills(stage); err != nil {
		return err
	}
	// This directory is exclusively generated. Canonical authoring files are read-only.
	if err := os.RemoveAll(p.destination); err != nil {
		return err
	}
	return os.Rename(stage, p.destination)
}

func (p packager) validateSkills(root string) error {
	for _, name := range []string{"datly-reader", "datly-writer", "datly-custom-component"} {
		if _, err := (skillformat.Compiler{Source: os.DirFS(filepath.Join(root, name))}).Compile(context.Background(), "skill://"+name+"/SKILL.md"); err != nil {
			return fmt.Errorf("skill %s: %w", name, err)
		}
	}
	return nil
}
