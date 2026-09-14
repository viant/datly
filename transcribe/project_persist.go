package transcribe

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const (
	projectMetadataDir  = ".datly"
	projectManifestFile = "project.json"
)

// projectMetadataStore owns all filesystem state for one project metadata
// root. ProjectGeneration owns synchronization and orchestration.
type projectMetadataStore struct {
	rootDir     string
	metadataDir string
}

func (s *projectMetadataStore) lock(ctx context.Context) (func(), error) {
	lockPath := filepath.Join(s.rootDir, ".datly.lock")
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	for {
		err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return func() {
				_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)
				_ = file.Close()
			}, nil
		}
		if err != unix.EWOULDBLOCK && err != unix.EAGAIN {
			_ = file.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			_ = file.Close()
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func newProjectMetadataStore(rootDir string) (*projectMetadataStore, error) {
	rootDir, err := filepath.Abs(strings.TrimSpace(rootDir))
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(rootDir) == "" {
		return nil, fmt.Errorf("project directory is required")
	}
	return &projectMetadataStore{rootDir: rootDir, metadataDir: filepath.Join(rootDir, projectMetadataDir)}, nil
}

func (s *projectMetadataStore) prepare() error {
	if err := s.ensureDirectory(s.metadataDir); err != nil {
		return err
	}
	for _, directory := range []string{"ir", "diagnostics"} {
		if err := s.ensureParent(filepath.Join(s.metadataDir, directory)); err != nil {
			return err
		}
	}
	return nil
}

func (s *projectMetadataStore) readManifest() (*ProjectManifest, error) {
	data, err := os.ReadFile(filepath.Join(s.metadataDir, projectManifestFile))
	if os.IsNotExist(err) {
		return &ProjectManifest{}, nil
	}
	if err != nil {
		return nil, err
	}
	result := &ProjectManifest{}
	if err = json.Unmarshal(data, result); err != nil {
		return nil, fmt.Errorf("decode project manifest: %w", err)
	}
	if result.Version != 0 && result.Version != projectManifestVersion {
		return nil, fmt.Errorf("unsupported project manifest version %d", result.Version)
	}
	return result, nil
}

func (s *projectMetadataStore) persist(manifest *ProjectManifest, snapshots map[string]any) error {
	if manifest == nil {
		return fmt.Errorf("project manifest is required")
	}
	if err := s.ensureDirectory(s.metadataDir); err != nil {
		return err
	}
	for relative, value := range snapshots {
		managed, err := managedProjectPath(relative)
		if err != nil {
			return err
		}
		destination := filepath.Join(s.metadataDir, managed)
		if err = s.ensureParent(filepath.Dir(destination)); err != nil {
			return err
		}
		if err = s.writeJSON(destination, value); err != nil {
			return err
		}
	}
	if err := s.writeJSON(filepath.Join(s.metadataDir, "migration.json"), manifest.Analysis); err != nil {
		return err
	}
	return s.writeJSON(filepath.Join(s.metadataDir, projectManifestFile), manifest)
}

func (s *projectMetadataStore) ensureParent(parent string) error {
	relative, err := filepath.Rel(s.metadataDir, parent)
	if err != nil {
		return err
	}
	if relative == "." {
		return nil
	}
	relative, err = managedProjectPath(relative)
	if err != nil {
		return err
	}
	current := s.metadataDir
	for _, segment := range strings.Split(relative, string(filepath.Separator)) {
		current = filepath.Join(current, segment)
		info, statErr := os.Lstat(current)
		if os.IsNotExist(statErr) {
			if err = os.Mkdir(current, 0o755); err != nil && !os.IsExist(err) {
				return err
			}
			continue
		}
		if statErr != nil {
			return statErr
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("project metadata directory %q is an unsupported symlink", current)
		}
		if !info.IsDir() {
			return fmt.Errorf("project metadata path %q is not a directory", current)
		}
	}
	return nil
}

func (s *projectMetadataStore) ensureDirectory(target string) error {
	if info, err := os.Lstat(target); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("project metadata directory %q is an unsupported symlink", target)
		}
		if !info.IsDir() {
			return fmt.Errorf("project metadata path %q is not a directory", target)
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	return os.MkdirAll(target, 0o755)
}

func (s *projectMetadataStore) writeJSON(path string, value any) error {
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("project metadata file %q is an unsupported symlink", path)
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	temporary, err := os.CreateTemp(filepath.Dir(path), ".datly-project-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if _, err = temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err = temporary.Chmod(0o644); err != nil {
		_ = temporary.Close()
		return err
	}
	if err = temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}
