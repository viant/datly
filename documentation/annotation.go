package docs

// Annotation contains documentation only. Nonempty authored values always win.
type Annotation struct{ Description, Example string }

// Field supplies identity from the existing schema/view projection owners.
type Field struct {
	Path, Name, Table, Column string
	Authored                  Annotation
}

func (s *Snapshot) Field(f Field) Annotation {
	result := f.Authored
	if s == nil {
		return result
	}
	if f.Column != "" {
		if result.Description == "" {
			result.Description, _ = s.columns.column(f.Table, f.Column)
		}
		if result.Example == "" {
			result.Example, _ = s.columns.column(f.Table, f.Column+"$example")
		}
	}
	if result.Description == "" {
		result.Description, _ = s.paths.byName(f.Path)
	}
	if result.Description == "" {
		result.Description, _ = s.paths.byName(f.Name)
	}
	if result.Example == "" {
		result.Example, _ = s.paths.byName(f.Path + "$example")
	}
	return result
}
func (s *Snapshot) Parameter(name string, authored Annotation) Annotation {
	if s == nil {
		return authored
	}
	if authored.Description == "" {
		authored.Description, _ = s.parameters.byName(name)
	}
	if authored.Description == "" {
		authored.Description, _ = s.filter.byName(name)
	}
	if authored.Example == "" {
		authored.Example, _ = s.parameters.byName(name + "$example")
	}
	if authored.Example == "" {
		authored.Example, _ = s.filter.byName(name + "$example")
	}
	return s.Field(Field{Path: name, Name: name, Authored: authored})
}
func (s *Snapshot) Operation(path string, authored Annotation) Annotation {
	return s.Field(Field{Path: path, Name: path, Authored: authored})
}
