package spec

import (
	"fmt"
	"regexp"
)

var protectedFlushTableName = regexp.MustCompile(`^[a-z_][a-z0-9_]*(?:[./][a-z_][a-z0-9_]*)*$`)

// ValidateProtectedFlushTables checks component-local exact table authority.
// Nil leaves ordinary flush behavior unchanged. Authored DQL canonicalizes case
// before constructing Settings; canonical metadata itself must be normalized.
func (s *Settings) ValidateProtectedFlushTables() error {
	if s == nil || s.ProtectedFlushTables == nil {
		return nil
	}
	if len(s.ProtectedFlushTables) == 0 {
		return fmt.Errorf("protected_flush_tables requires at least one table")
	}
	seen := make(map[string]bool, len(s.ProtectedFlushTables))
	for _, table := range s.ProtectedFlushTables {
		if !protectedFlushTableName.MatchString(table) {
			return fmt.Errorf("protected_flush_tables table %q must be a normalized exact identifier with optional dot or slash qualifiers", table)
		}
		if seen[table] {
			return fmt.Errorf("protected_flush_tables table %q is declared more than once", table)
		}
		seen[table] = true
	}
	return nil
}
