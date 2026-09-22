package managed

import (
	"context"
	"fmt"
	"github.com/viant/afs/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/viant/afs"
)

type FileStore struct {
	fs       afs.Service
	location string
}

func NewFileStore(location string) *FileStore {
	return &FileStore{fs: afs.New(), location: strings.TrimRight(location, "/")}
}
func (s *FileStore) Read(ctx context.Context) (Generation, error) {
	result := Generation{}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	for scope, target := range map[Scope]*string{All: &result.All, Lazy: &result.Lazy, Warmup: &result.Warmup} {
		location := s.location + "/" + string(scope)
		exists, err := s.fs.Exists(ctx, location)
		if err != nil {
			return result, err
		}
		if !exists {
			*target = "0"
			continue
		}
		data, err := s.fs.DownloadWithURL(ctx, location)
		if err != nil {
			return result, err
		}
		token := string(data)
		if _, err := uuid.Parse(token); err != nil {
			return result, fmt.Errorf("invalid cache generation at %s: %w", location, err)
		}
		*target = token
	}
	return result, nil
}
func (s *FileStore) Rotate(ctx context.Context, scope Scope) (string, error) {
	if err := scope.Validate(); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	token := uuid.NewString()
	if scheme := url.Scheme(s.location, "file"); scheme == "file" {
		directory := url.Path(s.location)
		if err := os.MkdirAll(directory, 0700); err != nil {
			return "", err
		}
		file, err := os.CreateTemp(directory, ".generation-")
		if err != nil {
			return "", err
		}
		defer os.Remove(file.Name())
		if _, err = file.WriteString(token); err != nil {
			_ = file.Close()
			return "", err
		}
		if err = file.Close(); err != nil {
			return "", err
		}
		if err = ctx.Err(); err != nil {
			return "", err
		}
		return token, os.Rename(file.Name(), filepath.Join(directory, string(scope)))
	}
	err := s.fs.Upload(ctx, s.location+"/"+string(scope), 0600, strings.NewReader(token))
	return token, err
}
