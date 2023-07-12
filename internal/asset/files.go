package asset

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"strings"
)

type Files []*File

func (f *Files) Append(file *File) {
	*f = append(*f, file)
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
	return fs.Upload(ctx, URL, file.DefaultFileOsMode, strings.NewReader(content))
}
