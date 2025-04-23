package shared

import (
	"fmt"
	"os"
	"path/filepath"
)

func FindModulePath(packagePath string) (string, error) {
	currentPath := packagePath
	for {
		modPath := filepath.Join(currentPath, "go.mod")
		if _, err := os.Stat(modPath); err == nil {
			return filepath.Rel(currentPath, packagePath)
		}

		parent := filepath.Dir(currentPath)
		if parent == "/" || parent == "" {
			return "", fmt.Errorf("failed to find module path")
		}
		currentPath = parent
	}
}
