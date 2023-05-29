package options

import "strings"

type Arguments []string

func (a Arguments) IsHelp() bool {
	for _, arg := range a {
		if arg == "-h" {
			return true
		}
	}
	return false
}

func (a Arguments) IsLegacy() bool {
	for _, arg := range a {
		if arg == "-l" {
			return true
		}
	}
	return false
}
func (a Arguments) SubMode() bool {
	if len(a) == 0 {
		return false
	}
	return !strings.HasPrefix(a[0], "-")
}
