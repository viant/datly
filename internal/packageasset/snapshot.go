package packageasset

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"path"
	"sort"
)

// Snapshotter creates a completed filesystem for one unpublished generation.
// Standard-library archive storage retains all bytes without fabricating embed internals.
type Snapshotter struct {
	Source  fs.FS
	Overlay []File
}

// File is a virtual source file supplied before an immutable snapshot is published.
type File struct {
	Path string
	Data []byte
}

func (s Snapshotter) All(ctx context.Context) (fs.FS, error) {
	if ctx == nil || (s.Source == nil && len(s.Overlay) == 0) {
		return nil, fmt.Errorf("resource snapshot context and source are required")
	}
	var files []string
	var err error
	if s.Source != nil {
		err = fs.WalkDir(s.Source, ".", func(name string, entry fs.DirEntry, err error) error {
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
	}
	if err != nil {
		return nil, fmt.Errorf("enumerate resource snapshot: %w", err)
	}
	return s.Files(ctx, files)
}

func (s Snapshotter) Files(ctx context.Context, files []string) (fs.FS, error) {
	if ctx == nil || (s.Source == nil && len(s.Overlay) == 0) {
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
	overlays := map[string]int{}
	for i, file := range s.Overlay {
		if !fs.ValidPath(file.Path) || file.Path == "." {
			return nil, fmt.Errorf("invalid overlay file %q", file.Path)
		}
		if _, ok := overlays[file.Path]; ok {
			return nil, fmt.Errorf("duplicate overlay file %q", file.Path)
		}
		overlays[file.Path] = i
		if !seen[file.Path] {
			paths = append(paths, file.Path)
			seen[file.Path] = true
		}
	}
	sort.Strings(paths)
	for _, name := range paths {
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			if seen[parent] {
				return nil, fmt.Errorf("resource file %q is also a parent directory", parent)
			}
		}
	}
	var buffer bytes.Buffer
	archive := zip.NewWriter(&buffer)
	for _, name := range paths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var content []byte
		if index, ok := overlays[name]; ok {
			content = s.Overlay[index].Data
		} else {
			info, err := fs.Stat(s.Source, name)
			if err != nil {
				return nil, fmt.Errorf("stat resource %q: %w", name, err)
			}
			if !info.Mode().IsRegular() {
				return nil, fmt.Errorf("resource %q is not a regular file", name)
			}
			content, err = fs.ReadFile(s.Source, name)
			if err != nil {
				return nil, fmt.Errorf("read resource %q: %w", name, err)
			}
		}
		writer, err := archive.CreateHeader(&zip.FileHeader{Name: name, Method: zip.Store})
		if err != nil {
			return nil, err
		}
		if _, err = writer.Write(content); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := archive.Close(); err != nil {
		return nil, err
	}
	storage := bytes.NewReader(buffer.Bytes())
	reader, err := zip.NewReader(storage, int64(buffer.Len()))
	if err != nil {
		return nil, err
	}
	result := &snapshotFS{Reader: reader, storage: storage, files: make(map[string]snapshotEntry, len(reader.File))}
	for _, file := range reader.File {
		offset, err := file.DataOffset()
		if err != nil {
			return nil, err
		}
		result.files[file.Name] = snapshotEntry{info: file.FileInfo(), offset: offset}
	}
	return result, nil
}
