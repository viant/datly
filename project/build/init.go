package build

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	xmodule "github.com/viant/x/module"
	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
)

type InitRequest struct {
	Dir, Module string
	// Pins are exact module versions; mutable queries such as latest are rejected.
	Pins map[string]string
	// Local explicitly maps module paths to development directories. Local-only
	// requirements use v0.0.0 placeholders, never asserted published versions.
	Local map[string]string
}

func (Service) Init(ctx context.Context, request InitRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	root, err := filepath.Abs(request.Dir)
	if err != nil {
		return err
	}
	name := filepath.Join(root, "go.mod")
	data, err := os.ReadFile(name)
	exists := err == nil
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	var file *modfile.File
	if exists {
		file, err = modfile.Parse(name, data, nil)
		if err != nil {
			return err
		}
		if initialized, err := (Service{}).initialized(root, file); err != nil {
			return err
		} else if initialized {
			return nil
		}
		if file.Module == nil {
			return fmt.Errorf("module directive is required")
		}
		if request.Module != "" && request.Module != file.Module.Mod.Path {
			return fmt.Errorf("existing module %s differs from requested %s", file.Module.Mod.Path, request.Module)
		}
	} else {
		if request.Module == "" {
			return fmt.Errorf("module path is required for a new project")
		}
		file = new(modfile.File)
		_ = file.AddModuleStmt(request.Module)
		_ = file.AddGoStmt("1.25.8")
	}
	if err = module.CheckPath(request.Module); err != nil && request.Module != "" {
		return err
	}
	modified := !exists
	keys := make([]string, 0, len(request.Pins)+len(request.Local))
	seen := map[string]bool{}
	for key := range request.Pins {
		seen[key] = true
		keys = append(keys, key)
	}
	for key := range request.Local {
		if !seen[key] {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		pin := request.Pins[key]
		local := request.Local[key]
		if pin != "" {
			if err := module.Check(key, pin); err != nil {
				return fmt.Errorf("exact pin %s: %w", key, err)
			}
			if module.CanonicalVersion(pin) != pin {
				return fmt.Errorf("exact canonical pin required for %s", key)
			}
		}
		required := ""
		for _, r := range file.Require {
			if r.Mod.Path == key {
				required = r.Mod.Version
			}
		}
		if required != "" && pin != "" && required != pin {
			return fmt.Errorf("preserving existing %s@%s: requested pin conflicts", key, required)
		}
		if local != "" {
			if !filepath.IsAbs(local) {
				local = filepath.Join(root, local)
			}
			local, err = filepath.Abs(local)
			if err != nil {
				return err
			}
			info, err := xmodule.LocateLocal(local)
			if err != nil {
				return err
			}
			if info.Path != key || info.Dir != local {
				return fmt.Errorf("local mapping %s must point to its module root", key)
			}
			found := false
			for _, r := range file.Replace {
				if r.Old.Path == key {
					actual := r.New.Path
					if !filepath.IsAbs(actual) {
						actual = filepath.Join(root, actual)
					}
					if r.Old.Version != "" || r.New.Version != "" || filepath.Clean(actual) != local {
						return fmt.Errorf("preserving existing replacement for %s: local mapping conflicts", key)
					}
					found = true
				}
			}
			if !found {
				if err = file.AddReplace(key, "", local, ""); err != nil {
					return err
				}
				modified = true
			}
			if pin == "" && required == "" {
				_, major, ok := module.SplitPathVersion(key)
				if !ok {
					return fmt.Errorf("invalid module path %s", key)
				}
				pin = "v0.0.0"
				if major != "" {
					pin = major[1:] + ".0.0"
				}
			}
		}
		if required == "" && pin != "" {
			if err = file.AddRequire(key, pin); err != nil {
				return err
			}
			modified = true
		}
	}
	available := false
	for _, r := range file.Require {
		if r.Mod.Path == "github.com/viant/datly" {
			available = true
		}
	}
	// Existing workspace modules are valid dependency authority without new pins.
	if !available && !exists {
		return fmt.Errorf("new module needs an exact github.com/viant/datly pin or explicit local mapping")
	}
	if err = os.MkdirAll(root, 0755); err != nil {
		return err
	}
	if modified {
		data, err = file.Format()
		if err != nil {
			return err
		}
		if err = os.WriteFile(name, data, 0644); err != nil {
			return err
		}
	}
	modulePath := file.Module.Mod.Path
	files := map[string]string{
		"cmd/datly/main.go":          fmt.Sprintf(mainTemplate, modulePath),
		"internal/datlylink/link.go": linkTemplate,
		"dql/README.md":              "Place authored DQL here; transcribe it into generated Go component packages before building.\n",
		"generated/README.md":        "Generated component holders and shapes. Add selected package holders to internal/datlylink.\n",
		"hooks/README.md":            "Authored Go hook packages. Reference exported factories from component handler metadata.\n",
		"resources/README.md":        "Keep package assets with their owning Go package and its existing resource manifest.\n",
		"datly.yaml":                 fmt.Sprintf("BaseDir: .\nEndpoint:\n  Address: 127.0.0.1:8080\nGoBootstrap:\n  Packages: [%s/...]\n", modulePath),
	}
	for _, path := range []string{"cmd/datly/main.go", "internal/datlylink/link.go", "dql/README.md", "generated/README.md", "hooks/README.md", "resources/README.md", "datly.yaml"} {
		if err = (Service{}).create(root, path, files[path]); err != nil {
			return err
		}
	}
	return nil
}

// initialized recognizes a custom Datly module from its stable project files.
// Transient build linkage is never persisted in the application module.
func (Service) initialized(root string, file *modfile.File) (bool, error) {
	if file.Module == nil {
		return false, nil
	}
	if err := module.CheckPath(file.Module.Mod.Path); err != nil {
		return false, fmt.Errorf("existing module path: %w", err)
	}
	custom := false
	for _, required := range file.Require {
		if required.Mod.Path == "github.com/viant/datly" {
			custom = true
			break
		}
	}
	if !custom {
		return false, nil
	}
	for _, relative := range []string{"cmd/datly/main.go", "internal/datlylink/link.go", "datly.yaml"} {
		if _, err := os.Stat(filepath.Join(root, relative)); err != nil {
			if os.IsNotExist(err) {
				return false, nil
			}
			return false, err
		}
	}
	return true, nil
}

func (Service) create(root, path, content string) error {
	name := filepath.Join(root, path)
	if err := os.MkdirAll(filepath.Dir(name), 0755); err != nil {
		return err
	}
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if os.IsExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	_, writeErr := f.WriteString(content)
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

const mainTemplate = `package main

import (
	"context"
	_ "%s/internal/datlylink"
	"github.com/viant/datly/cmd/command"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit((command.Service{}).Run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}
`

const linkTemplate = `// Package datlylink owns the application's explicit component imports.
// Add one blank import for each generated component package selected by the host.
package datlylink

func init() {}
`
