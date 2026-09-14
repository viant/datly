// Package differ adapts native godiff comparisons to the public handler contract.
package differ

import (
	"context"
	"fmt"
	"github.com/viant/godiff"
	xdiffer "github.com/viant/xdatly/differ"
	"reflect"
)

// Service owns the native comparator registry. Applications can share a service
// through handler.Capabilities; comparison options and results are invocation-local.
type Service struct{ registry *godiff.Registry }

func New() *Service { return &Service{registry: godiff.NewRegistry()} }

func (s *Service) Diff(ctx context.Context, from, to any, opts ...xdiffer.Option) (*xdiffer.ChangeLog, error) {
	if ctx == nil {
		return nil, fmt.Errorf("comparison context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil || s.registry == nil {
		return nil, fmt.Errorf("comparison service is not initialized")
	}
	fromType, toType := reflect.TypeOf(from), reflect.TypeOf(to)
	if fromType == nil && toType == nil {
		return &xdiffer.ChangeLog{}, nil
	}
	if fromType == nil {
		fromType = toType
	}
	if toType == nil {
		toType = fromType
	}
	comparator, err := s.registry.Get(fromType, toType, &godiff.Tag{})
	if err != nil {
		return nil, err
	}
	options := xdiffer.Options{}
	options.Apply(opts...)
	changes := comparator.Diff(from, to, godiff.WithShallow(options.WithShallow), godiff.WithSetMarker(options.WithSetMarker))
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := &xdiffer.ChangeLog{}
	for _, change := range changes.Changes {
		result.Changes = append(result.Changes, &xdiffer.Change{Type: xdiffer.ChangeType(change.Type), Path: s.path(change.Path), From: change.From, To: change.To, Error: change.Error})
	}
	return result, result.Err()
}

func (s *Service) path(source *godiff.Path) *xdiffer.Path {
	if source == nil {
		return nil
	}
	return &xdiffer.Path{Kind: xdiffer.PathKind(source.Kind), Path: s.path(source.Path), Name: source.Name, Index: source.Index, Key: source.Key}
}

var _ xdiffer.Differ = (*Service)(nil)
