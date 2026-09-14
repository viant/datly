package transcribe

import (
	"fmt"
	"path/filepath"
	"strings"
)

func managedProjectPath(path string) (string, error) {
	path = filepath.Clean(strings.TrimSpace(path))
	if path == "." || path == "" {
		return "", fmt.Errorf("path is required")
	}
	if filepath.IsAbs(path) || path == ".." || strings.HasPrefix(path, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path %q escapes project directory", path)
	}
	return path, nil
}
