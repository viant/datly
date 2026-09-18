package generate

import "fmt"

// ReadIndexSource is generated application support owned by the input package.
// Keeping it there lets Input.Init use typed indexes without importing its
// handler package. Linked contracts use a component-owned free accessor.
type ReadIndexSource struct {
	Package              string
	TypeName, CacheField string
	Source               MutationSource
}

func (s *ReadIndexSource) resolve(plan *Plan) error {
	if s == nil {
		return nil
	}
	source, err := s.Source.resolve(plan, s.Package)
	if err != nil {
		return err
	}
	if s.CacheField != "" {
		if plan.Input.Ownership != ContractGenerated {
			return fmt.Errorf("read index storage requires a generated input")
		}
		if _, exists := plan.Input.Field(s.CacheField); exists {
			return fmt.Errorf("generated read index storage collides with input field %s", s.CacheField)
		}
		plan.Input.Fields = append(plan.Input.Fields, Field{Name: s.CacheField, Type: "*" + s.TypeName, Implementation: true, Tag: `json:"-" sqlx:"-"`})
	}
	plan.ReadIndexes = &ReadIndexSource{Package: s.Package, TypeName: s.TypeName, CacheField: s.CacheField, Source: source}
	return nil
}
