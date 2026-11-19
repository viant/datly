// Package gobuild validates source-backed registrations using the application's
// Go build context. It compiles packages, never runs their initialization code.
package gobuild

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"go/importer"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// Context preserves the module/workspace, replacements and build selection.
// Env supplements the inherited environment. Empty Tags retains GOFLAGS;
// nonempty Tags explicitly selects the build tags for both validation stages.
type Context struct {
	Dir  string
	Tags string
	Env  []string
	ctx  context.Context
}

func (c *Context) Clone() *Context {
	if c == nil {
		return nil
	}
	r := *c
	r.Env = append([]string(nil), c.Env...)
	return &r
}

func (c *Context) WithContext(ctx context.Context) *Context {
	r := c.Clone()
	if r == nil {
		r = &Context{}
	}
	r.ctx = ctx
	return r
}

func (c *Context) run(args ...string) ([]byte, error) {
	ctx := c.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = c.Dir
	cmd.Env = append(os.Environ(), c.Env...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("handler source build: %w: %s", err, stderr.String())
	}
	return out, nil
}

func (c *Context) flags() ([]string, error) {
	data, err := c.run("env", "GOFLAGS")
	if err != nil {
		return nil, err
	}
	var args []string
	// Modern Go defaults list/build to readonly or vendor mode. Forcing
	// readonly unconditionally would silently bypass the application's vendor
	// tree. Only disallow an explicit request to update the module graph.
	mode := ""
	for _, flag := range strings.Fields(string(data)) {
		if value, ok := strings.CutPrefix(strings.Trim(flag, "\"'"), "-mod="); ok {
			mode = value
		}
	}
	if mode == "mod" {
		args = append(args, "-mod=readonly")
	}
	if c.Tags != "" {
		args = append(args, "-tags", c.Tags)
	}
	return args, nil
}

// Importer reads Go's build-selected export data. A single importer preserves
// identity across contracts, aliases and the actual xdatly generic interface.
func (c *Context) Importer(packages ...string) (types.Importer, error) {
	flags, err := c.flags()
	if err != nil {
		return nil, err
	}
	args := append([]string{"list", "-export", "-deps", "-json"}, flags...)
	args = append(args, "--")
	data, err := c.run(append(args, packages...)...)
	if err != nil {
		return nil, err
	}
	exports := map[string]string{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	for {
		var pkg struct{ ImportPath, Export string }
		if err := decoder.Decode(&pkg); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		exports[pkg.ImportPath] = pkg.Export
	}
	return importer.ForCompiler(token.NewFileSet(), "gc", func(path string) (io.ReadCloser, error) {
		file := exports[path]
		if file == "" {
			return nil, fmt.Errorf("no compiled export data for %q", path)
		}
		return os.Open(file)
	}), nil
}

// Validate overlays staged files at their final locations, so internal-package
// access and import cycles are checked in the real destination package context.
// A nil content removes a file. No application files or module files are written.
func (c *Context) Validate(pkg string, files map[string][]byte) error {
	_, err := c.ValidateFiles(pkg, files)
	return err
}

// ValidateFiles also returns the build-selected source filenames so metadata
// loading cannot accidentally reintroduce excluded files after publication.
func (c *Context) ValidateFiles(pkg string, files map[string][]byte) ([]string, error) {
	return c.sourceFiles(pkg, files, true)
}

// GoFiles selects staged source using the same build context as ValidateFiles.
// It does not type-check; publication must still pass ValidateFiles.
func (c *Context) GoFiles(pkg string, files map[string][]byte) ([]string, error) {
	return c.sourceFiles(pkg, files, false)
}

func (c *Context) sourceFiles(pkg string, files map[string][]byte, validate bool) ([]string, error) {
	selected, err := c.projectedFiles(map[string]map[string][]byte{pkg: files}, validate)
	return selected[pkg], err
}

// ValidatePackages compiles the destinations in one overlay. Each package's
// proposed files must belong to its directory in the selected Go build.
func (c *Context) ValidatePackages(files map[string]map[string][]byte) (map[string][]string, error) {
	return c.projectedFiles(files, true)
}

func (c *Context) projectedFiles(byPackage map[string]map[string][]byte, validate bool) (map[string][]string, error) {
	dir, err := os.MkdirTemp("", "datly-handler-build-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	replace, err := c.inheritedOverlay()
	if err != nil {
		return nil, err
	}
	packages := make([]string, 0, len(byPackage))
	for pkg := range byPackage {
		packages = append(packages, pkg)
	}
	sort.Strings(packages)
	if len(packages) == 0 {
		return nil, fmt.Errorf("no build packages supplied")
	}
	i := 0
	for _, pkg := range packages {
		for name, content := range byPackage[pkg] {
			logical, err := c.absolutePath(name)
			if err != nil {
				return nil, err
			}
			physical, err := physicalPath(logical)
			if err != nil {
				return nil, err
			}
			if content == nil {
				replace[logical], replace[physical] = "", ""
				continue
			}
			backing := filepath.Join(dir, fmt.Sprintf("%d", i))
			i++
			if err = os.WriteFile(backing, content, 0600); err != nil {
				return nil, err
			}
			// Explicit go.work paths may retain the logical spelling while the
			// process working directory uses the physical module path.
			replace[logical], replace[physical] = backing, backing
		}
	}
	data, err := json.Marshal(struct{ Replace map[string]string }{replace})
	if err != nil {
		return nil, err
	}
	overlay := filepath.Join(dir, "overlay.json")
	if err = os.WriteFile(overlay, data, 0600); err != nil {
		return nil, err
	}
	// A second checkout can have the same module path. Never validate its old
	// package while publishing files to a different destination outside this graph.
	flags, err := c.flags()
	if err != nil {
		return nil, err
	}
	listArgs := append([]string{"list", "-json"}, flags...)
	listArgs = append(listArgs, "-overlay", overlay, "--")
	listed, err := c.run(append(listArgs, packages...)...)
	if err != nil {
		return nil, err
	}
	selected := make(map[string][]string, len(packages))
	decoder := json.NewDecoder(bytes.NewReader(listed))
	for {
		var target struct {
			ImportPath, Dir   string
			GoFiles, CgoFiles []string
		}
		if err := decoder.Decode(&target); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		key := target.ImportPath
		if len(packages) == 1 {
			key = packages[0] // Single-package callers can use a relative package argument.
		}
		if _, seen := selected[key]; seen {
			return nil, fmt.Errorf("build selector %q resolved multiple packages", key)
		}
		files, ok := byPackage[key]
		if !ok || target.Dir == "" {
			return nil, fmt.Errorf("unexpected build destination %q (%s)", target.ImportPath, target.Dir)
		}
		packageDir, err := physicalPath(target.Dir)
		if err != nil {
			return nil, err
		}
		for name := range files {
			logical, err := c.absolutePath(name)
			if err != nil {
				return nil, err
			}
			file, err := physicalPath(logical)
			if err != nil {
				return nil, err
			}
			rel, err := filepath.Rel(packageDir, file)
			if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				return nil, fmt.Errorf("staged handler file %s is outside selected build package %s; use the destination module's Go build context", name, packageDir)
			}
		}
		selected[key] = append(target.GoFiles, target.CgoFiles...)
	}
	if len(selected) != len(packages) {
		return nil, fmt.Errorf("Go resolved %d of %d build packages", len(selected), len(packages))
	}
	if validate {
		args := append([]string{"build"}, flags...)
		args = append(args, "-overlay", overlay, "-o", filepath.Join(dir, "package.a"), "--")
		for _, pkg := range packages {
			if _, err = c.run(append(args, pkg)...); err != nil {
				return nil, err
			}
		}
	}
	return selected, nil
}

// Go canonicalizes its module directory. Resolve existing ancestors too when
// the endpoint directory does not exist yet (notably /var -> /private/var).
func physicalPath(name string) (string, error) {
	abs, err := filepath.Abs(name)
	if err != nil {
		return "", err
	}
	ancestor := abs
	var suffix []string
	for {
		resolved, err := filepath.EvalSymlinks(ancestor)
		if err == nil {
			for i := len(suffix) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, suffix[i])
			}
			return resolved, nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(ancestor)
		if parent == ancestor {
			return "", err
		}
		suffix = append(suffix, filepath.Base(ancestor))
		ancestor = parent
	}
}
