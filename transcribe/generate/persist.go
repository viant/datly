package generate

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// legacyManifestName is only removed during migration; its contents are never read.
const legacyManifestName = ".datly-gen.json"

type scaffoldPersistence struct {
	dir       string
	owner     string
	files     []EmittedFile
	userFiles []EmittedFile
	removals  []string
	plan      *Plan
	renames   map[string]bool
}

type scaffoldCommitLock struct {
	mutex sync.Mutex
	users int
}

type scaffoldCommitLocks struct {
	mutex sync.Mutex
	items map[string]*scaffoldCommitLock
}

var scaffoldLocks = scaffoldCommitLocks{items: map[string]*scaffoldCommitLock{}}

func (l *scaffoldCommitLocks) acquire(target string) func() {
	l.mutex.Lock()
	item := l.items[target]
	if item == nil {
		item = &scaffoldCommitLock{}
		l.items[target] = item
	}
	item.users++
	l.mutex.Unlock()

	item.mutex.Lock()
	return func() {
		item.mutex.Unlock()
		l.mutex.Lock()
		item.users--
		if item.users == 0 {
			delete(l.items, target)
		}
		l.mutex.Unlock()
	}
}

func (p *scaffoldPersistence) Commit() error {
	target, err := p.target()
	if err != nil {
		return err
	}
	release := scaffoldLocks.acquire(target)
	defer release()
	parent := filepath.Dir(target)
	if err = os.MkdirAll(parent, 0o755); err != nil {
		return err
	}
	stage, err := os.MkdirTemp(parent, "."+filepath.Base(target)+"-stage-")
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.RemoveAll(stage)
		}
	}()
	// The copy doubles as the original snapshot: each file is read once, and
	// the fingerprint is taken from the bytes being copied.
	original, stats, err := p.copyExisting(target, stage)
	if err != nil {
		return err
	}
	if err = p.prepareCurrent(target); err != nil {
		return err
	}
	if err = p.validateUserFiles(target, stage); err != nil {
		return err
	}
	for name := range p.renames {
		if err = os.Remove(filepath.Join(stage, name)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	if err = p.writeFiles(target, stage); err != nil {
		return err
	}
	if err = p.writeUserFiles(target, stage); err != nil {
		return err
	}
	if err = os.Remove(filepath.Join(stage, legacyManifestName)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if p.plan != nil && p.plan.ExternalHandler != nil && p.plan.ExternalHandler.Build != nil {
		if err = p.validateHandlerStage(target, stage); err != nil {
			return err
		}
	}
	if err = p.swap(target, stage, original, stats); err != nil {
		return err
	}
	committed = true
	return nil
}

func (p *scaffoldPersistence) validateHandlerStage(target, stage string) error {
	files := map[string][]byte{}
	// Explicit removals are part of the build overlay, not merely absent from
	// the proposal. Otherwise stale generated files can mask an invalid update.
	err := filepath.WalkDir(target, func(name string, entry fs.DirEntry, err error) error {
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		files[name] = nil
		return nil
	})
	if err != nil {
		return err
	}
	err = filepath.WalkDir(stage, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(stage, name)
		if err != nil {
			return err
		}
		content, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		files[filepath.Join(target, rel)] = content
		return nil
	})
	if err != nil {
		return err
	}
	p.plan.handlerGoFiles, err = p.plan.ExternalHandler.Build.ValidateFiles(p.plan.Package, files)
	return err
}

func (p *scaffoldPersistence) Validate() error {
	_, err := p.preview()
	return err
}

// preview exposes the exact current proposal to package/import validation.
func (p *scaffoldPersistence) preview() (*scaffoldPersistence, error) {
	copy := *p
	copy.files = append([]EmittedFile(nil), p.files...)
	target, err := copy.target()
	if err != nil {
		return nil, err
	}
	if _, err = os.Stat(target); err == nil {
		if err = validateScaffoldTree(target); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	if _, err = copy.desiredFiles(target); err != nil {
		return nil, err
	}
	if err = copy.validateUserFiles(target, target); err != nil {
		return nil, err
	}
	if err = copy.prepareCurrent(target); err != nil {
		return nil, err
	}
	return &copy, nil
}

func (p *scaffoldPersistence) target() (string, error) {
	if p == nil || strings.TrimSpace(p.dir) == "" {
		return "", fmt.Errorf("scaffold directory is required")
	}
	if strings.TrimSpace(p.owner) == "" {
		return "", fmt.Errorf("scaffold component owner is required")
	}
	return filepath.Abs(p.dir)
}

func validateScaffoldTree(target string) error {
	return filepath.WalkDir(target, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("generated package contains unsupported symlink %q", path)
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return fmt.Errorf("generated package contains unsupported non-regular file %q", path)
		}
		return nil
	})
}

func (p *scaffoldPersistence) desiredFiles(target string) ([]string, error) {
	result := make([]string, 0, len(p.files))
	seen := map[string]bool{}
	for _, file := range p.files {
		relative, err := managedPath(target, file.Path)
		if err != nil {
			return nil, err
		}
		if seen[relative] {
			return nil, fmt.Errorf("generated file %q occurs more than once", relative)
		}
		seen[relative] = true
		result = append(result, relative)
	}
	sort.Strings(result)
	return result, nil
}

func (p *scaffoldPersistence) writeFiles(target, stage string) error {
	for _, file := range p.files {
		relative, err := managedPath(target, file.Path)
		if err != nil {
			return err
		}
		path := filepath.Join(stage, relative)
		if err = os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err = os.WriteFile(path, []byte(file.Content), 0o644); err != nil {
			return err
		}
	}
	return nil
}

func (p *scaffoldPersistence) writeUserFiles(target, stage string) error {
	for _, file := range p.userFiles {
		relative, err := managedPath(target, file.Path)
		if err != nil {
			return err
		}
		path := filepath.Join(stage, relative)
		info, err := os.Lstat(path)
		if err == nil {
			if !info.Mode().IsRegular() {
				return fmt.Errorf("user-owned scaffold %q is not a regular file", relative)
			}
			continue
		}
		if !os.IsNotExist(err) {
			return err
		}
		if err = os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err = os.WriteFile(path, []byte(file.Content), 0o644); err != nil {
			return err
		}
	}
	return nil
}

func (p *scaffoldPersistence) validateUserFiles(base, existing string) error {
	seen := map[string]bool{}
	for _, file := range p.userFiles {
		relative, err := managedPath(base, file.Path)
		if err != nil {
			return err
		}
		if seen[relative] {
			return fmt.Errorf("user-owned scaffold %q occurs more than once", relative)
		}
		seen[relative] = true
		path := filepath.Join(existing, relative)
		info, err := os.Lstat(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("user-owned scaffold %q is not a regular file", relative)
		}
		if p.plan != nil && p.plan.HookScaffold != nil && relative == p.plan.HookScaffold.Destination {
			if err = validateExistingHookScaffold(path, p.plan); err != nil {
				return fmt.Errorf("user-owned hook scaffold %q: %w", relative, err)
			}
		}
	}
	return nil
}

// copyExisting mirrors the current package into the stage and returns the
// snapshot of what was copied, so publication can detect concurrent edits
// without re-reading the tree.
func (p *scaffoldPersistence) copyExisting(target, stage string) (scaffoldSnapshot, scaffoldStats, error) {
	snapshot, stats := scaffoldSnapshot{}, scaffoldStats{}
	info, err := os.Lstat(target)
	if os.IsNotExist(err) {
		return snapshot, stats, nil
	}
	if err != nil {
		return nil, nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, nil, fmt.Errorf("scaffold target %q is an unsupported symlink", target)
	}
	if !info.IsDir() {
		return nil, nil, fmt.Errorf("scaffold target %q is not a directory", target)
	}
	err = filepath.WalkDir(target, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(target, path)
		if err != nil || relative == "." {
			return err
		}
		destination := filepath.Join(stage, relative)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("generated package contains unsupported symlink %q", path)
		}
		if entry.IsDir() {
			snapshot[relative] = snapshotEntry(info, nil)
			return os.MkdirAll(destination, info.Mode().Perm())
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("generated package contains unsupported non-regular file %q", path)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		snapshot[relative] = snapshotEntry(info, data)
		stats[relative] = scaffoldStat{size: info.Size(), modTime: info.ModTime()}
		return os.WriteFile(destination, data, info.Mode().Perm())
	})
	if err != nil {
		return nil, nil, err
	}
	return snapshot, stats, nil
}

func (p *scaffoldPersistence) swap(target, stage string, original scaffoldSnapshot, stats scaffoldStats) error {
	info, err := os.Lstat(target)
	if os.IsNotExist(err) {
		if len(original) != 0 {
			return fmt.Errorf("generated package changed during staging: target was removed")
		}
		return os.Rename(stage, target)
	} else if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("scaffold target %q is an unsupported symlink", target)
	}
	if !info.IsDir() {
		return fmt.Errorf("generated package changed during staging: target is not a directory")
	}
	backup, err := os.MkdirTemp(filepath.Dir(target), "."+filepath.Base(target)+"-backup-")
	if err != nil {
		return err
	}
	if err = os.Remove(backup); err != nil {
		return err
	}
	if err = os.Rename(target, backup); err != nil {
		return err
	}
	if err = original.validate(backup, stats); err != nil {
		if restoreErr := os.Rename(backup, target); restoreErr != nil {
			return fmt.Errorf("%w; restore package: %v", err, restoreErr)
		}
		return err
	}
	if err = os.Rename(stage, target); err != nil {
		if restoreErr := os.Rename(backup, target); restoreErr != nil {
			return fmt.Errorf("commit scaffold: %w; restore previous package: %v", err, restoreErr)
		}
		return err
	}
	if err = os.RemoveAll(backup); err != nil {
		if moveErr := os.Rename(target, stage); moveErr != nil {
			return fmt.Errorf("remove scaffold backup: %w; preserve new package for recovery: %v", err, moveErr)
		}
		if restoreErr := os.Rename(backup, target); restoreErr != nil {
			return fmt.Errorf("remove scaffold backup: %w; restore previous package: %v", err, restoreErr)
		}
		return fmt.Errorf("remove scaffold backup: %w", err)
	}
	return nil
}

func managedPath(target, path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	relative, err := filepath.Rel(target, absolute)
	if err != nil {
		return "", err
	}
	return managedRelativePath(relative)
}

func managedRelativePath(path string) (string, error) {
	path = filepath.Clean(strings.TrimSpace(path))
	if path == "." || path == "" {
		return "", nil
	}
	if filepath.IsAbs(path) || path == ".." || strings.HasPrefix(path, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("generated path %q escapes package directory", path)
	}
	return path, nil
}
