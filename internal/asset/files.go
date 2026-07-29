package asset

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"go/format"
	"strings"
)

type Files []*File

func (f *Files) Append(file *File) {
	*f = append(*f, file)
}

func (f *Files) Reset() {
	*f = (*f)[:0]
}

func (f Files) Upload(ctx context.Context, fs afs.Service) error {
	for _, aFile := range f {
		if err := aFile.Validate(); err != nil {
			return err
		}
		if err := f.uploadContent(ctx, fs, aFile.URL, aFile.Content); err != nil {
			return err
		}
	}
	return nil
}

func (f Files) uploadContent(ctx context.Context, fs afs.Service, URL string, content string) error {
	_ = fs.Delete(ctx, URL)
	baseURL, _ := url.Split(URL, file.Scheme)
	if ok, _ := fs.Exists(ctx, baseURL); !ok {
		_ = fs.Create(ctx, baseURL, file.DefaultDirOsMode, true)
	}
	// gofmt generated Go files so their layout is stable across regenerations.
	// Non-Go files and not-yet-valid Go are written through unchanged.
	if strings.HasSuffix(URL, ".go") {
		if formatted, err := format.Source([]byte(content)); err == nil {
			content = string(formatted)
		}
	}
	return fs.Upload(ctx, URL, file.DefaultFileOsMode, strings.NewReader(content))
}
