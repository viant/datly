package options

import (
	"github.com/viant/afs/url"
	"os"
	"strings"
)

func ensureAbsPath(location string) string {
	if strings.Contains(location, "~") {
		location = strings.Replace(location, "~", os.Getenv("HOME"), 1)
	}
	if location == "" {
		return location
	}
	if !url.IsRelative(location) {
		return location
	}

	if wd, _ := os.Getwd(); wd != "" {
		return url.Join(wd, location)
	}
	return location
}
