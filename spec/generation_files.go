package spec

import (
	"fmt"
	"go/token"
	"path/filepath"
	"strings"
)

// SetSQLFile selects a generated SQL resource path for root or a named field/view role.
func (s *GenerationSettings) SetSQLFile(role, file string) error {
	role, file = strings.TrimSpace(role), strings.TrimSpace(file)
	if role == "" || file == "" {
		return fmt.Errorf("sql_dest requires a nonempty role and destination")
	}
	if strings.Contains(file, "\\") || filepath.IsAbs(file) || filepath.Ext(file) != ".sql" {
		return fmt.Errorf("sql_dest role %q requires a relative .sql destination", role)
	}
	for _, part := range strings.Split(filepath.ToSlash(file), "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("sql_dest role %q has an invalid destination %q", role, file)
		}
	}
	if s.SQLFiles == nil {
		s.SQLFiles = map[string]string{}
	}
	key := strings.ToLower(role)
	if _, exists := s.SQLFiles[key]; exists {
		return fmt.Errorf("duplicate sql_dest role %q", role)
	}
	s.SQLFiles[key] = filepath.ToSlash(file)
	return nil
}

// SQLFile returns a configured SQL resource path or its readable default.
func (s *GenerationSettings) SQLFile(role, fallback string) string {
	if s != nil {
		if file := s.SQLFiles[strings.ToLower(strings.TrimSpace(role))]; file != "" {
			return file
		}
	}
	return fallback
}

// SQLFileFor returns an exact role override or inherits the root view's
// configured directory while retaining the role's default filename.
func (s *GenerationSettings) SQLFileFor(role, rootRole, fallback string) string {
	if s == nil {
		return fallback
	}
	if exact := s.SQLFile(role, ""); exact != "" {
		return exact
	}
	if root := s.SQLFile(rootRole, ""); root != "" && fallback != "" {
		directory := filepath.Dir(root)
		if directory == "." {
			return filepath.Base(fallback)
		}
		return filepath.ToSlash(filepath.Join(directory, filepath.Base(fallback)))
	}
	return fallback
}

// SetSupportFile selects a named support product without depending on its filename.
func (s *GenerationSettings) SetSupportFile(role, file string) error {
	if strings.TrimSpace(file) == "" {
		return fmt.Errorf("support_dest role %q requires a nonempty destination", role)
	}
	switch role {
	case "entities", "entity_methods", "types", "input_setters", "setters", "frames", "previous", "layout", "actions", "mutation_output", "validation", "hooks", "invariants", "indexes":
	default:
		name, ok := strings.CutPrefix(role, "type:")
		if !ok || !token.IsIdentifier(name) || !token.IsExported(name) {
			return fmt.Errorf("unknown support_dest role %q", role)
		}
	}
	if s.SupportFiles == nil {
		s.SupportFiles = map[string]string{}
	}
	if _, exists := s.SupportFiles[role]; exists {
		return fmt.Errorf("duplicate support_dest role %q", role)
	}
	s.SupportFiles[role] = file
	return nil
}

// File returns a transcription destination, or the caller's role-specific default.
func (s *GenerationSettings) File(role, fallback string) string {
	if s == nil {
		return fallback
	}
	var file string
	switch role {
	case "view":
		file = s.ViewFile
	case "input":
		file = s.InputFile
	case "output":
		file = s.OutputFile
	case "router":
		file = s.RouterFile
	case "handler":
		file = s.HandlerFile
	case "lifecycle":
		file = s.LifecycleFile
	case "mutation":
		file = s.MutationFile
	case "resources":
		file = s.ResourcesFile
	case "links":
		file = s.LinksFile
	case "template":
		file = s.TemplateFile
	default:
		file = s.SupportFiles[role]
	}
	if file != "" {
		return file
	}
	if fallback == "" {
		return ""
	}
	return s.FilePrefix + fallback
}

// ValidateFilePrefix restricts the optional filename prefix to a visible,
// package-local filename fragment. Empty means prefix-free defaults.
func (s *GenerationSettings) ValidateFilePrefix() error {
	if s == nil || s.FilePrefix == "" {
		return nil
	}
	for i, r := range s.FilePrefix {
		letter := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z'
		if !letter && (i == 0 || !(r >= '0' && r <= '9' || r == '_' || r == '-')) {
			return fmt.Errorf("file_prefix %q must start with an ASCII letter and contain only letters, digits, underscores or hyphens", s.FilePrefix)
		}
	}
	return nil
}
