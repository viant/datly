package generate

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
)

type scaffoldFileSnapshot struct {
	fingerprint string
	mode        os.FileMode
}
type scaffoldSnapshot map[string]scaffoldFileSnapshot

// readScaffoldSnapshot captures the copied package, including user-owned files.
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
		value := scaffoldFileSnapshot{mode: info.Mode()}
		if !entry.IsDir() {
			if !info.Mode().IsRegular() {
				return fmt.Errorf("generated package contains unsupported non-regular file %q", path)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			value.fingerprint = scaffoldFingerprint(data)
		}
		result[relative] = value
		return nil
	})
	return result, err
}

func (s scaffoldSnapshot) validate(directory string) error {
	current, err := readScaffoldSnapshot(directory)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(s, current) {
		return fmt.Errorf("generated package changed during staging; retry after preserving the latest edits")
	}
	return nil
}
