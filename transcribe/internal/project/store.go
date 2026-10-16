package project

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	MetadataDir  = ".datly"
	ManifestFile = "project.json"
)

// Store owns project metadata persistence and locking. The caller holds Lock
// across reading, component generation, and persistence.
type Store struct {
	rootDir     string
	metadataDir string
}

func (s *Store) Lock(ctx context.Context) (func(), error) {
	lockPath := filepath.Join(s.rootDir, ".datly.lock")
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	for {
		unlock, blocked, lockErr := tryLockFile(file)
		if lockErr == nil && !blocked {
			return func() {
				_ = unlock()
				_ = file.Close()
			}, nil
		}
		if lockErr != nil {
			_ = file.Close()
			return nil, lockErr
		}
		select {
		case <-ctx.Done():
			_ = file.Close()
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func NewStore(rootDir string) (*Store, error) {
	if strings.TrimSpace(rootDir) == "" {
		return nil, fmt.Errorf("project directory is required")
	}
	rootDir, err := filepath.Abs(strings.TrimSpace(rootDir))
	if err != nil {
		return nil, err
	}
	return &Store{rootDir: rootDir, metadataDir: filepath.Join(rootDir, MetadataDir)}, nil
}

func (s *Store) Prepare() error {
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

func (s *Store) ReadManifest() (*Manifest, error) {
	data, err := os.ReadFile(filepath.Join(s.metadataDir, ManifestFile))
	if os.IsNotExist(err) {
		return &Manifest{}, nil
	}
	if err != nil {
		return nil, err
	}
	result := &Manifest{}
	if err = json.Unmarshal(data, result); err != nil {
		return nil, fmt.Errorf("decode project manifest: %w", err)
	}
	if result.Version != 0 && result.Version != ManifestVersion {
		return nil, fmt.Errorf("unsupported project manifest version %d", result.Version)
	}
	return result, nil
}

func (s *Store) Persist(manifest *Manifest, snapshots map[string]any) error {
	if manifest == nil {
		return fmt.Errorf("project manifest is required")
	}
	if err := s.ensureDirectory(s.metadataDir); err != nil {
		return err
	}
	for relative, value := range snapshots {
		managed, err := ManagedPath(relative)
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
	return s.writeJSON(filepath.Join(s.metadataDir, ManifestFile), manifest)
}

func (s *Store) ensureParent(parent string) error {
	relative, err := filepath.Rel(s.metadataDir, parent)
	if err != nil {
		return err
	}
	if relative == "." {
		return nil
	}
	relative, err = ManagedPath(relative)
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

func (s *Store) ensureDirectory(target string) error {
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

func (s *Store) writeJSON(path string, value any) error {
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
