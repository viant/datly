package build

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

func linkPackageClause(dir, fallback string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil && !os.IsNotExist(err) {
		return "", err
	}
	name := ""
	for _, entry := range entries {
		fileName := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(fileName, ".go") || strings.HasSuffix(fileName, "_test.go") || strings.HasPrefix(fileName, ".") || strings.HasPrefix(fileName, "_") {
			continue
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("symlinked link source is not supported: %s", filepath.Join(dir, fileName))
		}
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, fileName), nil, parser.PackageClauseOnly)
		if err != nil {
			return "", err
		}
		if err := validateLinkName(file.Name.Name); err != nil {
			return "", err
		}
		if name != "" && name != file.Name.Name {
			return "", fmt.Errorf("link directory contains mixed package clauses: %s", dir)
		}
		name = file.Name.Name
	}
	if name == "" {
		name = fallback
	}
	return name, validateLinkName(name)
}

type plannedLinkFile struct {
	path              string
	original, updated []byte
	exists            bool
	mode              os.FileMode
}

type linkPlan struct {
	root, dir, overlay string
	replacements       map[string]string
	files              map[string]*plannedLinkFile
}

func newLinkPlan(root string) (*linkPlan, error) {
	dir, err := os.MkdirTemp("", "datly-link-plan-*")
	if err != nil {
		return nil, err
	}
	return &linkPlan{root: root, dir: dir, overlay: filepath.Join(dir, "overlay.json"), replacements: map[string]string{}, files: map[string]*plannedLinkFile{}}, nil
}

func (p *linkPlan) close() { _ = os.RemoveAll(p.dir) }

func (p *linkPlan) file(path string) (*plannedLinkFile, error) {
	if file := p.files[path]; file != nil {
		return file, nil
	}
	relative, err := filepath.Rel(p.root, path)
	if err != nil {
		return nil, err
	}
	if err := validateLinkPath(p.root, relative); err != nil {
		return nil, err
	}
	file := &plannedLinkFile{path: relative, mode: 0644}
	info, err := os.Stat(path)
	if err == nil {
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("link destination is not a regular file: %s", path)
		}
		file.exists, file.mode = true, info.Mode().Perm()
		file.original, err = os.ReadFile(path)
		if err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	file.updated = bytes.Clone(file.original)
	p.files[path] = file
	return file, nil
}

func (p *linkPlan) read(path string) ([]byte, error) {
	if replacement := p.replacements[path]; replacement != "" {
		return os.ReadFile(replacement)
	}
	file, err := p.file(path)
	if err != nil {
		return nil, err
	}
	return file.original, nil
}

func (p *linkPlan) overlayFile(path string, content []byte) error {
	target := p.replacements[path]
	if target == "" {
		target = filepath.Join(p.dir, fmt.Sprintf("file-%d.go", len(p.replacements)))
	}
	if err := os.WriteFile(target, content, 0600); err != nil {
		return err
	}
	p.replacements[path] = target
	encoded, err := json.Marshal(struct{ Replace map[string]string }{p.replacements})
	if err != nil {
		return err
	}
	return os.WriteFile(p.overlay, encoded, 0600)
}

func (p *linkPlan) protectModuleFiles(ctx context.Context, env []string) ([]string, error) {
	paths := []string{filepath.Join(p.root, "go.mod"), filepath.Join(p.root, "go.sum")}
	cmd := exec.CommandContext(ctx, "go", "env", "GOWORK")
	cmd.Dir, cmd.Env = p.root, env
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("resolve link build workspace: %w: %s", err, output)
	}
	if work := strings.TrimSpace(string(output)); work != "" && work != "off" {
		parent, err := filepath.EvalSymlinks(filepath.Dir(work))
		if err != nil {
			return nil, err
		}
		work = filepath.Join(parent, filepath.Base(work))
		var normalized []string
		for _, entry := range env {
			if !strings.HasPrefix(entry, "GOWORK=") {
				normalized = append(normalized, entry)
			}
		}
		env = append(normalized, "GOWORK="+work)
		paths = append(paths, work, work+".sum")
	}
	// In workspace mode Go may update go.work.sum even with -mod=readonly.
	// Overlay module files too: Go explicitly refuses to update overlaid module
	// metadata, leaving the original graph and checksums untouched on failure.
	for _, path := range paths {
		content, err := os.ReadFile(path)
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		if err := p.overlayFile(path, content); err != nil {
			return nil, err
		}
	}
	return env, nil
}

func (p *linkPlan) validate(ctx context.Context, request LinkRequest, linkPackage string, env []string) error {
	for path, file := range p.files {
		if file.updated == nil {
			continue
		}
		if _, err := parser.ParseFile(token.NewFileSet(), path, file.updated, 0); err != nil {
			return fmt.Errorf("planned link file %s: %w", path, err)
		}
		if err := p.overlayFile(path, file.updated); err != nil {
			return err
		}
	}
	// Compile the complete planned link package and its dependencies. This
	// catches helper collisions, import cycles, and visibility violations without
	// running constructors or init functions, or publishing a temporary seed.
	args := []string{"build", "-overlay", p.overlay, "-o", filepath.Join(p.dir, "link.a")}
	if request.Tags != "" {
		args = append(args, "-tags", request.Tags)
	}
	args = append(args, "./"+linkPackage)
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir, cmd.Env = p.root, env
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("validate planned link package: %w\n%s", err, output)
	}
	return ctx.Err()
}

func (p *linkPlan) unchanged(root *os.Root, file *plannedLinkFile) error {
	if err := validateLinkPath(p.root, file.path); err != nil {
		return err
	}
	info, err := root.Stat(file.path)
	if !file.exists && os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("link destination changed during sync: %s: %w", file.path, err)
	}
	if !file.exists || info.Mode().Perm() != file.mode {
		return fmt.Errorf("link destination changed during sync: %s", file.path)
	}
	current, err := root.ReadFile(file.path)
	if err != nil {
		return err
	}
	if !bytes.Equal(current, file.original) {
		return fmt.Errorf("link destination changed during sync: %s", file.path)
	}
	return nil
}

// publish is deliberately not a multi-file transaction. All validation and
// initial concurrent-change checks precede writes; I/O failures report which
// replacements completed. Helpers precede the link file, so new imports are last.
func (p *linkPlan) publish(ctx context.Context, linkPath string) error {
	return p.publishWith(ctx, linkPath, publishLinkFile)
}

func (p *linkPlan) publishWith(ctx context.Context, linkPath string, write func(*os.Root, *plannedLinkFile) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	root, err := os.OpenRoot(p.root)
	if err != nil {
		return err
	}
	defer root.Close()
	var paths []string
	for path, file := range p.files {
		if err := p.unchanged(root, file); err != nil {
			return err
		}
		if !file.exists || !bytes.Equal(file.original, file.updated) {
			paths = append(paths, path)
		}
	}
	sort.Slice(paths, func(i, j int) bool {
		if paths[i] == linkPath {
			return false
		}
		if paths[j] == linkPath {
			return true
		}
		return paths[i] < paths[j]
	})
	var published []string
	for _, path := range paths {
		file := p.files[path]
		err = ctx.Err()
		if err == nil {
			err = p.unchanged(root, file)
		}
		if err == nil {
			err = write(root, file)
		}
		if err != nil {
			return fmt.Errorf("publish link file %s (already published: %v; directories may have been created): %w", file.path, published, err)
		}
		published = append(published, file.path)
	}
	return nil
}

func publishLinkFile(root *os.Root, file *plannedLinkFile) error {
	dir := filepath.Dir(file.path)
	if err := root.MkdirAll(dir, 0755); err != nil {
		return err
	}
	temp := filepath.Join(dir, ".datly-link-"+rand.Text())
	f, err := root.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, file.mode)
	if err != nil {
		return err
	}
	defer root.Remove(temp)
	err = f.Chmod(file.mode)
	if err == nil {
		_, err = f.Write(file.updated)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return root.Rename(temp, file.path)
}
