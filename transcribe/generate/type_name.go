package generate

import "strings"

func unwrapQualifiedTypeName(name string) string {
	name = strings.TrimSpace(name)
	for {
		switch {
		case strings.HasPrefix(name, "[]"):
			name = strings.TrimSpace(name[2:])
		case strings.HasPrefix(name, "*"):
			name = strings.TrimSpace(name[1:])
		default:
			return name
		}
	}
}
