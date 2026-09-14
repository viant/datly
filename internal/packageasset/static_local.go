package packageasset

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// localSnapshot traverses only single directory names from an independently
// trusted handle. Checking the identity after OpenRoot prevents a replacement
// between Lstat and OpenRoot from authorizing a different directory. Each child
// stays pinned while the next name is checked; no checked pathname is reopened.
func (s StaticSource) localSnapshot(ctx context.Context, local, subtree string) (*embed.FS, error) {
	if local == "" {
		return nil, fmt.Errorf("static local path must not be empty")
	}
	if ctx == nil {
		return nil, fmt.Errorf("static snapshot context is required")
	}
	root := s.LocalRoot
	owned := false
	if root != nil {
		if filepath.IsAbs(local) || filepath.VolumeName(local) != "" {
			return nil, fmt.Errorf("static local path must be relative to the supplied LocalRoot")
		}
	} else {
		anchor := "." // The process working directory is the default relative authority.
		if filepath.IsAbs(local) {
			anchor = filepath.VolumeName(local) + string(os.PathSeparator)
			local = strings.TrimPrefix(local, anchor)
		} else if filepath.VolumeName(local) != "" {
			return nil, fmt.Errorf("static volume-relative path is ambiguous")
		}
		var err error
		root, err = os.OpenRoot(anchor)
		if err != nil {
			return nil, err
		}
		owned = true
	}
	defer func() {
		if owned {
			_ = root.Close()
		}
	}()
	// Do not Clean: link/../site must not erase the untrusted link from validation.
	for _, part := range strings.Split(filepath.ToSlash(local)+"/"+subtree, "/") {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if part == "" || part == "." {
			continue
		}
		if part == ".." || !fs.ValidPath(part) || strings.Contains(part, "\\") {
			return nil, fmt.Errorf("static local path contains an invalid directory component")
		}
		info, err := root.Lstat(part)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() || info.Mode()&fs.ModeSymlink != 0 {
			return nil, fmt.Errorf("static local path contains a symlink or non-directory")
		}
		next, err := root.OpenRoot(part)
		if err != nil {
			return nil, err
		}
		opened, err := next.Stat(".")
		if err != nil || !os.SameFile(info, opened) {
			_ = next.Close()
			return nil, fmt.Errorf("static local directory changed while opening")
		}
		if owned {
			_ = root.Close()
		}
		root, owned = next, true
	}
	return (Snapshotter{Source: root.FS()}).Folder(ctx, ".")
}
