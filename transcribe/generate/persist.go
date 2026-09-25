package generate

import (
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/viant/datly/internal/packageasset"
)

const scaffoldManifestName = packageasset.ManifestName

const scaffoldManifestVersion = 5

type scaffoldManifest struct {
	Identity         string                       `json:"identity,omitempty"`
	ComponentPackage string                       `json:"componentPackage,omitempty"`
	Destinations     map[string]string            `json:"destinations,omitempty"`
	Owners           map[string]*scaffoldManifest `json:"owners,omitempty"`
	others           map[string]*scaffoldManifest
	Version          int                                  `json:"version"`
	Owner            string                               `json:"owner"`
	Files            []string                             `json:"files"`
	Roles            map[string]string                    `json:"roles,omitempty"`
	Resources        *ResourceManifest                    `json:"resources,omitempty"`
	Fingerprints     map[string]string                    `json:"fingerprints,omitempty"`
	ProjectionFields map[string]*projectionFieldOwnership `json:"projectionFields,omitempty"`
	exists           bool
}

type scaffoldPersistence struct {
	dir              string
	owner            string
	files            []EmittedFile
	userFiles        []EmittedFile
	removals         []string
	plan             *Plan
	renames          map[string]bool
	customizedShapes map[string]bool
	fieldOwnership   map[string]*projectionFieldOwnership
	proposal         []EmittedFile
	ephemeral        bool
	policy           GenerationPolicy
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
	p.prepareFiles()
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
	manifest, err := readScaffoldManifest(stage)
	if err != nil {
		return err
	}
	manifest, err = manifest.forOwner(p.owner, p.plan != nil && p.plan.GoPackage != "")
	if err != nil {
		return err
	}
	if err = p.validateExisting(target, stage, manifest); err != nil {
		return err
	}
	if err = p.validateUserFiles(target, stage); err != nil {
		return err
	}
	if err = p.preserveEntityMethods(target, stage, manifest); err != nil {
		return err
	}
	if err = p.mergeShapes(target, stage, manifest); err != nil {
		return err
	}
	desired, err := p.desiredFiles(target)
	if err != nil {
		return err
	}
	desired, roles, err := p.retainShapes(target, stage, manifest, desired)
	if err != nil {
		return err
	}
	resources, err := p.retainResources(target, stage, manifest, &desired, roles)
	if err != nil {
		return err
	}
	fingerprints, err := p.protectArtifacts(target, stage, manifest, desired)
	if err != nil {
		return err
	}
	if err = p.removeStale(stage, manifest, desired); err != nil {
		return err
	}
	if err = p.writeFiles(target, stage); err != nil {
		return err
	}
	if err = p.writeUserFiles(target, stage); err != nil {
		return err
	}
	metadata := p.destinationMetadata()
	metadata.Roles, metadata.Resources, metadata.Fingerprints, metadata.ProjectionFields, metadata.others = roles, resources, fingerprints, p.fieldOwnership, manifest.others
	if !p.ephemeral {
		if err = writeScaffoldManifest(stage, p.owner, desired, metadata); err != nil {
			return err
		}
	}
	if err = p.swap(target, stage, original, stats); err != nil {
		return err
	}
	committed = true
	return nil
}

func (p *scaffoldPersistence) Validate() error {
	_, err := p.preview()
	return err
}

// preview exposes the exact merged files to package/import validation without
// replacing the caller's raw generator proposal with retained authored edits.
func (p *scaffoldPersistence) preview() (*scaffoldPersistence, error) {
	copy := *p
	copy.prepareFiles()
	if err := copy.validatePrepared(); err != nil {
		return nil, err
	}
	return &copy, nil
}

func (p *scaffoldPersistence) validatePrepared() error {
	target, err := p.target()
	if err != nil {
		return err
	}
	info, err := os.Lstat(target)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("scaffold target %q is an unsupported symlink", target)
	}
	if !info.IsDir() {
		return fmt.Errorf("scaffold target %q is not a directory", target)
	}
	if err = validateScaffoldTree(target); err != nil {
		return err
	}
	manifest, err := readScaffoldManifest(target)
	if err != nil {
		return err
	}
	manifest, err = manifest.forOwner(p.owner, p.plan != nil && p.plan.GoPackage != "")
	if err != nil {
		return err
	}
	if err = p.validateExisting(target, target, manifest); err != nil {
		return err
	}
	if err = p.validateUserFiles(target, target); err != nil {
		return err
	}
	if err = p.preserveEntityMethods(target, target, manifest); err != nil {
		return err
	}
	if err = p.mergeShapes(target, target, manifest); err != nil {
		return err
	}
	desired, err := p.desiredFiles(target)
	if err != nil {
		return err
	}
	desired, roles, err := p.retainShapes(target, target, manifest, desired)
	if err != nil {
		return err
	}
	if _, err = p.retainResources(target, target, manifest, &desired, roles); err != nil {
		return err
	}
	_, err = p.protectArtifacts(target, target, manifest, desired)
	return err
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

func (p *scaffoldPersistence) validateExisting(_, existing string, manifest *scaffoldManifest) error {
	if manifest == nil {
		return fmt.Errorf("scaffold manifest is required")
	}
	if err := p.prepareRenames(existing, manifest); err != nil {
		return err
	}
	if err := p.validateForeignFiles(manifest); err != nil {
		return err
	}
	owned := map[string]bool{}
	if manifest.exists {
		if owner := strings.TrimSpace(manifest.Owner); owner != strings.TrimSpace(p.owner) {
			return fmt.Errorf("generated package is owned by component %q, not %q", owner, p.owner)
		}
		for _, candidate := range manifest.Files {
			relative, err := managedRelativePath(candidate)
			if err != nil {
				return err
			}
			if relative == "" {
				return fmt.Errorf("generated package manifest contains an empty file path")
			}
			owned[relative] = true
		}
	}
	generated, err := p.generatedPaths()
	if err != nil {
		return err
	}
	for _, candidate := range append(generated, p.removals...) {
		relative, err := managedRelativePath(candidate)
		if err != nil {
			return err
		}
		if relative == "" {
			continue
		}
		if _, err = os.Lstat(filepath.Join(existing, relative)); err == nil && !owned[relative] {
			return fmt.Errorf("generated file %q collides with an unowned package file", relative)
		} else if !os.IsNotExist(err) {
			if err == nil {
				continue
			}
			return err
		}
	}
	return nil
}

func (p *scaffoldPersistence) generatedPaths() ([]string, error) {
	result := make([]string, 0, len(p.files))
	for _, file := range p.files {
		relative, err := managedPath(p.dir, file.Path)
		if err != nil {
			return nil, err
		}
		if relative != "" {
			result = append(result, relative)
		}
	}
	return result, nil
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

func (p *scaffoldPersistence) removeStale(stage string, manifest *scaffoldManifest, desired []string) error {
	keep := make(map[string]bool, len(desired))
	for _, name := range desired {
		keep[name] = true
	}
	candidates := append([]string(nil), manifest.Files...)
	candidates = append(candidates, p.removals...)
	for _, candidate := range candidates {
		relative, err := managedRelativePath(candidate)
		if err != nil {
			return err
		}
		if relative == "" || keep[relative] {
			continue
		}
		if err = os.Remove(filepath.Join(stage, relative)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
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

func readScaffoldManifest(stage string) (*scaffoldManifest, error) {
	result, err := readScaffoldMetadata(stage)
	if err != nil || !result.exists {
		return result, err
	}
	if err := result.validateVersion(); err != nil {
		return nil, err
	}
	return result, nil
}

func (m *scaffoldManifest) validateVersion() error {
	if m.Version != scaffoldManifestVersion && m.Version != 4 && m.Version != 3 && m.Version != 2 {
		return fmt.Errorf("unsupported scaffold manifest version %d", m.Version)
	}
	return nil
}

func readScaffoldMetadata(stage string) (*scaffoldManifest, error) {
	data, err := os.ReadFile(filepath.Join(stage, scaffoldManifestName))
	if os.IsNotExist(err) {
		return &scaffoldManifest{}, nil
	}
	if err != nil {
		return nil, err
	}
	result := &scaffoldManifest{}
	if err = json.Unmarshal(data, result); err != nil {
		return nil, fmt.Errorf("decode scaffold manifest: %w", err)
	}
	result.exists = true
	return result, nil
}

func writeScaffoldManifest(stage, owner string, files []string, metadata ...*scaffoldManifest) error {
	manifest := &scaffoldManifest{Version: scaffoldManifestVersion, Owner: strings.TrimSpace(owner), Files: files}
	if len(metadata) > 0 && metadata[0] != nil {
		manifest.others = metadata[0].others
		manifest.Identity = metadata[0].Identity
		manifest.ComponentPackage = metadata[0].ComponentPackage
		manifest.Destinations = metadata[0].Destinations
		manifest.Roles = metadata[0].Roles
		manifest.Resources = metadata[0].Resources
		manifest.Fingerprints = metadata[0].Fingerprints
		manifest.ProjectionFields = metadata[0].ProjectionFields
	}
	data, err := json.MarshalIndent(manifest.aggregate(), "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(filepath.Join(stage, scaffoldManifestName), data, 0o644)
}

func copyScaffoldFile(source, destination string, mode os.FileMode) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	if _, err = io.Copy(output, input); err != nil {
		_ = output.Close()
		return err
	}
	return output.Close()
}
