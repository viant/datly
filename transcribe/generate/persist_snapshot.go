package generate

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

type scaffoldFileSnapshot struct {
	fingerprint string
	mode        os.FileMode
}

// scaffoldSnapshot is content identity only (fingerprint and mode), so two
// snapshots of byte-identical trees compare equal regardless of timestamps.
type scaffoldSnapshot map[string]scaffoldFileSnapshot

// scaffoldStat is the metadata fast path recorded while copying: a file whose
// size and modification time are unchanged is accepted without re-reading it.
type scaffoldStat struct {
	size    int64
	modTime time.Time
}
type scaffoldStats map[string]scaffoldStat

func snapshotEntry(info os.FileInfo, data []byte) scaffoldFileSnapshot {
	value := scaffoldFileSnapshot{mode: info.Mode()}
	if !info.IsDir() {
		value.fingerprint = scaffoldFingerprint(data)
	}
	return value
}

// readScaffoldSnapshot captures a package tree, including user-owned files.
// Publication compares the renamed original before exposing staged artifacts,
// so edits made while compilation was staging are not silently discarded.
func readScaffoldSnapshot(directory string) (scaffoldSnapshot, error) {
	result := scaffoldSnapshot{}
	err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(directory, path)
		if err != nil {
			return err
		}
		if relative == "." {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		var data []byte
		if !entry.IsDir() {
			if !info.Mode().IsRegular() {
				return fmt.Errorf("generated package contains unsupported non-regular file %q", path)
			}
			if data, err = os.ReadFile(path); err != nil {
				return err
			}
		}
		result[relative] = snapshotEntry(info, data)
		return nil
	})
	return result, err
}

// validate confirms the directory still matches the snapshot. With stats from
// the copy, files whose size and modification time are unchanged are accepted
// without reading them; anything that differs (or lacks stats) is re-read and
// compared by fingerprint, so the concurrent-edit guarantee is unchanged while
// the common case avoids reading every file a second time.
func (s scaffoldSnapshot) validate(directory string, stats scaffoldStats) error {
	changed := fmt.Errorf("generated package changed during staging; retry after preserving the latest edits")
	seen := 0
	err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(directory, path)
		if err != nil {
			return err
		}
		if relative == "." {
			return nil
		}
		expected, ok := s[relative]
		if !ok {
			return changed
		}
		seen++
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode() != expected.mode {
			return changed
		}
		if entry.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("generated package contains unsupported non-regular file %q", path)
		}
		if stat, ok := stats[relative]; ok && info.Size() == stat.size && info.ModTime().Equal(stat.modTime) {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if scaffoldFingerprint(data) != expected.fingerprint {
			return changed
		}
		return nil
	})
	if err != nil {
		return err
	}
	if seen != len(s) {
		return changed
	}
	return nil
}
