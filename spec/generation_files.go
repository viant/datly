package spec

import (
	"fmt"
	"go/token"
	"strings"
)

// SetSupportFile selects a named support product without depending on its filename.
func (s *GenerationSettings) SetSupportFile(role, file string) error {
	if strings.TrimSpace(file) == "" {
		return fmt.Errorf("support_dest role %q requires a nonempty destination", role)
	}
	switch role {
	case "entities", "entity_methods", "types", "frames", "previous", "layout", "actions", "mutation_output", "validation", "hooks", "invariants":
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
