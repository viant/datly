package build

import (
	"context"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	xmodule "github.com/viant/x/module"
	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
)

type InitRequest struct {
	Dir, Module string
	// LinkPackage is module-relative; the default is internal/dependencylink.
	LinkPackage string
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
	linkPackage, linkName, err := resolveLinkPackage(request.LinkPackage)
	if err != nil {
		return err
	}
	if linkPackage == "cmd/datly" {
		return fmt.Errorf("link package conflicts with cmd/datly entrypoint")
	}
	if err := validateLinkPath(root, filepath.Join(filepath.FromSlash(linkPackage), "link.go")); err != nil {
		return err
	}
	linkName, err = linkPackageClause(filepath.Join(root, filepath.FromSlash(linkPackage)), linkName)
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
		if initialized, err := (Service{}).initialized(root, file, linkPackage); err != nil {
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
	if err := verifyExistingEntrypoint(root, file.Module.Mod.Path, linkPackage); err != nil {
		return err
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
		"cmd/datly/main.go":      fmt.Sprintf(mainTemplate, modulePath, linkPackage),
		linkPackage + "/link.go": fmt.Sprintf(linkTemplate, linkName),
		"dql/README.md":          "Place authored DQL here; transcribe it into generated Go component packages before building.\n",
		"generated/README.md":    fmt.Sprintf("Generated component holders and shapes. Add selected package holders to %s.\n", linkPackage),
		"hooks/README.md":        "Authored Go hook packages. Reference exported factories from component handler metadata.\n",
		"resources/README.md":    "Keep package assets with their owning Go package and its existing resource manifest.\n",
		"datly.yaml":             fmt.Sprintf("BaseDir: .\nEndpoint:\n  Address: 127.0.0.1:8080\nGoBootstrap:\n  Packages: [%s/...]\n", modulePath),
	}
	for _, path := range []string{"cmd/datly/main.go", linkPackage + "/link.go", "dql/README.md", "generated/README.md", "hooks/README.md", "resources/README.md", "datly.yaml"} {
		if err = (Service{}).create(root, path, files[path]); err != nil {
			return err
		}
	}
	return nil
}

func verifyExistingEntrypoint(root, modulePath, linkPackage string) error {
	path := filepath.Join(root, "cmd/datly/main.go")
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	want := modulePath + "/" + linkPackage
	for _, imported := range file.Imports {
		actual, err := strconv.Unquote(imported.Path.Value)
		if err != nil {
			return err
		}
		if actual == want && imported.Name != nil && imported.Name.Name == "_" {
			return nil
		}
	}
	return fmt.Errorf("existing cmd/datly/main.go must blank-import %q before initializing the requested link package", want)
}

// initialized recognizes a custom Datly module from its stable project files.
// Transient build linkage is never persisted in the application module.
func (Service) initialized(root string, file *modfile.File, linkPackage string) (bool, error) {
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
	for _, relative := range []string{"cmd/datly/main.go", "datly.yaml"} {
		if _, err := os.Stat(filepath.Join(root, relative)); err != nil {
			if os.IsNotExist(err) {
				return false, nil
			}
			return false, err
		}
	}
	if _, err := os.Stat(filepath.Join(root, linkPackage, "link.go")); err == nil {
		return true, nil
	} else if !os.IsNotExist(err) {
		return false, err
	}
	// Older initialized projects keep their existing application-owned link
	// package until the caller explicitly chooses to migrate it.
	if linkPackage == DefaultLinkPackage {
		if _, err := os.Stat(filepath.Join(root, "internal/datlylink/link.go")); err == nil {
			return true, nil
		} else if !os.IsNotExist(err) {
			return false, err
		}
	}
	return false, nil
}

func (Service) create(root, path, content string) error {
	dir, err := os.OpenRoot(root)
	if err != nil {
		return err
	}
	defer dir.Close()
	if err := dir.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := dir.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
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
	_ "%s/%s"
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

const linkTemplate = `package %s
`
