package dql

import (
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/mod/module"
)

// SetPackage replaces the one authored package directive while preserving all
// surrounding source bytes. Expected enables revision-aware callers to verify
// that package authority has not changed since inspection.
func SetPackage(source, packagePath, expected string) (string, error) {
	packagePath = strings.TrimSpace(packagePath)
	if err := module.CheckImportPath(packagePath); err != nil {
		return "", fmt.Errorf("invalid package path %q: %w", packagePath, err)
	}
	offset := 0
	found := false
	for _, line := range strings.SplitAfter(source, "\n") {
		lineBody := strings.TrimSuffix(line, "\n")
		lineBody = strings.TrimSuffix(lineBody, "\r")
		current, ok := parsePackageLineDirective(strings.TrimSpace(lineBody))
		if !ok {
			offset += len(line)
			continue
		}
		if found {
			return "", fmt.Errorf("multiple package directives are not supported")
		}
		found = true
		if strings.TrimSpace(expected) != "" && current != strings.TrimSpace(expected) {
			return "", fmt.Errorf("package changed: got %q, expected %q", current, strings.TrimSpace(expected))
		}
		indent := lineBody[:len(lineBody)-len(strings.TrimLeft(lineBody, " \t"))]
		replacement := indent + "#package(" + strconv.Quote(packagePath) + ")"
		if strings.HasSuffix(line, "\r\n") {
			replacement += "\r\n"
		} else if strings.HasSuffix(line, "\n") {
			replacement += "\n"
		}
		result, err := ApplyPatch(source, SourceSpan{Start: offset, End: offset + len(line)}, replacement)
		if err != nil {
			return "", err
		}
		return result, nil
	}
	if !found {
		return "", fmt.Errorf("package directive was not found")
	}
	return "", fmt.Errorf("package directive was not updated")
}
