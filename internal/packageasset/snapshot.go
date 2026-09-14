package packageasset

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"path"
	"sort"

	afsembed "github.com/viant/afs/embed"
)

// Snapshotter creates a completed filesystem for one unpublished generation.
// Its private holder is never returned or reused after publication.
type Snapshotter struct{ Source fs.FS }

func (s Snapshotter) All(ctx context.Context) (*embed.FS, error) {
	if ctx == nil || s.Source == nil {
		return nil, fmt.Errorf("resource snapshot context and source are required")
	}
	var files []string
	err := fs.WalkDir(s.Source, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !entry.IsDir() {
			files = append(files, name)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("enumerate resource snapshot: %w", err)
	}
	return s.Files(ctx, files)
}

func (s Snapshotter) Files(ctx context.Context, files []string) (*embed.FS, error) {
	if ctx == nil || s.Source == nil {
		return nil, fmt.Errorf("resource snapshot context and source are required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	paths := append([]string(nil), files...)
	sort.Strings(paths)
	seen := map[string]bool{}
	for _, name := range paths {
		if !fs.ValidPath(name) || name == "." {
			return nil, fmt.Errorf("invalid resource file path %q", name)
		}
		if seen[name] {
			return nil, fmt.Errorf("duplicate resource file %q", name)
		}
		seen[name] = true
	}
	for _, name := range paths {
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			if seen[parent] {
				return nil, fmt.Errorf("resource file %q is also a parent directory", parent)
			}
		}
	}
	holder := afsembed.NewHolder()
	for _, name := range paths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		info, err := fs.Stat(s.Source, name)
		if err != nil {
			return nil, fmt.Errorf("stat resource %q: %w", name, err)
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("resource %q is not a regular file", name)
		}
		content, err := fs.ReadFile(s.Source, name)
		if err != nil {
			return nil, fmt.Errorf("read resource %q: %w", name, err)
		}
		holder.Add(name, string(content))
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return holder.EmbedFs(), nil
}
