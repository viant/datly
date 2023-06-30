package command

import (
	"fmt"
	goFormat "go/format"
	"path"
)

type File struct {
	Content string
	URL     string
}

func (f *File) validate() error {
	switch path.Ext(f.URL) {
	case ".go":
		return f.ensureValidGo()
	}
	return nil
}

func (f *File) ensureValidGo() error {
	source, err := goFormat.Source([]byte(f.Content))
	if err != nil {
		return fmt.Errorf("invalid go file: %v, %w: %s", f.URL, err, f.Content)
	}
	f.Content = string(source)
	return nil
}

func NewFile(URL string, content string) *File {
	return &File{URL: URL, Content: content}
}
