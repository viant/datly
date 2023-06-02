package command

import (
	"bytes"
	"context"
	"github.com/viant/afs/option"
	"github.com/viant/datly/cmd/options"
	"path"
)

func (s *Service) Touch(ctx context.Context, touch *options.Touch) {
	if touch.RoutesURL == "" {
		return
	}
	if objects, err := s.fs.List(ctx, touch.RoutesURL, option.NewRecursive(true)); err == nil {
		for _, object := range objects {
			if object.IsDir() {
				continue
			}
			ext := path.Ext(object.Name())
			if ext == ".yaml" || ext == ".yml" {
				if data, err := s.fs.DownloadWithURL(ctx, object.URL()); err == nil {
					_ = s.fs.Upload(ctx, object.URL(), object.Mode(), bytes.NewReader(data))
				}
			}
		}
	}
	return
}
