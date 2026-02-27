package parity

import (
	"fmt"
	"os"
	"strings"
)

// resolveConnectors returns connectors from env override, or defaults.
// When DATLY_PARITY_SQLITE_DSN is set, all default connector names are mapped to sqlite3.
func resolveConnectors(defaults []string) []string {
	if override := splitNonEmpty(os.Getenv("DATLY_PARITY_CONNECTORS")); len(override) > 0 {
		return override
	}
	sqliteDSN := strings.TrimSpace(os.Getenv("DATLY_PARITY_SQLITE_DSN"))
	if sqliteDSN == "" {
		return defaults
	}
	ret := make([]string, 0, len(defaults))
	for _, item := range defaults {
		parts := strings.Split(item, "|")
		if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
			continue
		}
		ret = append(ret, fmt.Sprintf("%s|sqlite3|%s", strings.TrimSpace(parts[0]), sqliteDSN))
	}
	return ret
}
