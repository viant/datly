package generate

import (
	"fmt"
	"path/filepath"

	"github.com/viant/datly/typecatalog"
)

// ReadIndexSource holds the builder and optional input cache. Helpers is set
// when typed lookup declarations are emitted in a separate package.
type ReadIndexSource struct {
	Package              string
	TypeName, CacheField string
	Source               MutationSource
	Helpers              *MutationSource
}

func (s *ReadIndexSource) clone() (*ReadIndexSource, error) {
	result := *s
	var err error
	result.Source, err = s.Source.clone()
	if err != nil {
		return nil, err
	}
	if s.Helpers != nil {
		helpers, err := s.Helpers.clone()
		if err != nil {
			return nil, err
		}
		result.Helpers = &helpers
	}
	return &result, nil
}

func (s *ReadIndexSource) resolve(plan *Plan) error {
	if s == nil {
		return nil
	}
	source, err := s.Source.resolve(plan, s.Package)
	if err != nil {
		return err
	}
	resolved := *s
	resolved.Source = source
	if s.CacheField != "" {
		if plan.Input.Ownership != ContractGenerated {
			authority, err := typecatalog.NewDestinationAuthority(plan.ProjectRoot)
			if err == nil {
				destination, resolveErr := authority.Resolve(s.Package, ".")
				if resolveErr == nil && RetainsReadIndexCache(filepath.Join(plan.ProjectRoot, destination.Directory), source.Destination, plan.Input.Destination, plan.ComponentName, plan.Input.Type, s.CacheField, s.TypeName, source.File) {
					plan.ReadIndexes = &resolved
					return nil
				}
			}
			return fmt.Errorf("read index storage requires a generated input")
		}
		if _, exists := plan.Input.Field(s.CacheField); exists {
			return fmt.Errorf("generated read index storage collides with input field %s", s.CacheField)
		}
		plan.Input.Fields = append(plan.Input.Fields, Field{Name: s.CacheField, Type: "*" + s.TypeName, Implementation: true, Tag: `json:"-" sqlx:"-"`})
	}
	plan.ReadIndexes = &resolved
	return nil
}
