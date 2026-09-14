package standalone

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/viant/datly/gateway/openapi"
)

// Startup exports are snapshots, not a file watcher. Render all documents before
// writing any, and replace each local destination atomically with private mode.
func (s *Server) exportDocuments(ctx context.Context) error {
	if s.source.http.OpenAPI == nil {
		return nil
	}
	type artifact struct {
		path string
		data []byte
	}
	var artifacts []artifact
	for _, request := range s.source.http.OpenAPI.StartupExports {
		data, err := s.manager.ExportOpenAPI(ctx, openapi.ExportRequest{Path: request.Path, Format: request.Format})
		if err != nil {
			return err
		}
		location, err := url.Parse(request.URL)
		if err != nil {
			return fmt.Errorf("invalid OpenAPI export destination")
		}
		artifacts = append(artifacts, artifact{path: location.Path, data: data})
	}
	for _, item := range artifacts {
		if err := ctx.Err(); err != nil {
			return err
		}
		file, err := os.CreateTemp(filepath.Dir(item.path), ".datly-openapi-*")
		if err != nil {
			return fmt.Errorf("create OpenAPI export failed")
		}
		name := file.Name()
		_, writeErr := file.Write(item.data)
		closeErr := file.Close()
		if writeErr == nil && closeErr == nil && ctx.Err() == nil {
			err = os.Rename(name, item.path)
		} else {
			err = fmt.Errorf("write OpenAPI export failed")
		}
		_ = os.Remove(name)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			return fmt.Errorf("publish OpenAPI export failed")
		}
	}
	return nil
}
