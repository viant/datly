package loader

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/goccy/go-yaml"
	"github.com/viant/afs"
	"github.com/viant/datly/oas/openapi3"
	"io"
)

type Service struct {
	fs afs.Service
}

func (s *Service) LoadURL(ctx context.Context, URL string) (*openapi3.OpenAPI, error) {
	reader, err := s.fs.OpenURL(ctx, URL)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	object, _ := s.fs.Object(ctx, URL)
	return s.Load(ctx, object.URL(), reader)
}

func (s *Service) Load(ctx context.Context, location string, reader io.Reader) (*openapi3.OpenAPI, error) {
	var err error
	ctx, session, newSession := s.session(ctx)
	root := &openapi3.OpenAPI{}
	if err = s.loadURL(ctx, reader, root); err != nil {
		return nil, fmt.Errorf("failed to load from %v, err: %w ", location, err)
	}
	if session.Location == "" {
		session.Location = location
	}
	if newSession {
		err = session.Close()
	}
	return root, err
}

func (s *Service) loadURL(ctx context.Context, reader io.Reader, target interface{}) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	isJSON := bytes.HasPrefix(data, []byte("{"))
	if isJSON {
		return json.Unmarshal(data, target)
	}
	return yaml.UnmarshalContext(ctx, data, target)
}

func (s *Service) session(ctx context.Context) (context.Context, *openapi3.Session, bool) {
	session := openapi3.LookupSession(ctx)
	if session != nil {
		return ctx, session, false
	}
	ctx = openapi3.NewSessionContext(ctx)
	session = openapi3.LookupSession(ctx)
	return ctx, session, true
}

//New creates a loader service
func New(fs afs.Service) *Service {
	return &Service{fs: fs}
}
