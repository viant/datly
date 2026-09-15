package packageasset

import (
	"context"
	"fmt"
	"io/fs"
	"path"
	"strings"
)

// Folder snapshots an explicit subtree for both generated embedding and MCP.
// Filesystems supplied directly must be immutable; development OS callers use os.Root.
func (s Snapshotter) Folder(ctx context.Context, root string) (fs.FS, error) {
	if ctx == nil || s.Source == nil {
		return nil, fmt.Errorf("resource source and context are required")
	}
	if root == "" {
		root = "."
	}
	if !fs.ValidPath(root) || strings.Contains(root, "\\") {
		return nil, fmt.Errorf("invalid resource root")
	}
	parent := "."
	if root != "." {
		for _, part := range strings.Split(root, "/") {
			entries, err := fs.ReadDir(s.Source, parent)
			if err != nil {
				return nil, err
			}
			found := false
			for _, entry := range entries {
				if entry.Name() == part {
					found = true
					if !entry.IsDir() || entry.Type()&fs.ModeSymlink != 0 {
						return nil, fmt.Errorf("resource root contains symlink or non-directory")
					}
					break
				}
			}
			if !found {
				return nil, fs.ErrNotExist
			}
			parent = path.Join(parent, part)
		}
	}
	source, err := fs.Sub(s.Source, root)
	if err != nil {
		return nil, err
	}
	var files []string
	var total int64
	err = fs.WalkDir(source, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		if entry.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("resource tree contains a symlink")
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() > 4<<20 {
			return fmt.Errorf("unsupported or oversized resource file")
		}
		total += info.Size()
		files = append(files, name)
		if total > 32<<20 || len(files) > 4096 {
			return fmt.Errorf("resource tree exceeds publication limits")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return (Snapshotter{Source: source}).Files(ctx, files)
}
