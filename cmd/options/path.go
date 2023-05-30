package options

import (
	"github.com/viant/afs/url"
	"os"
)

func ensureAbsPath(location string) string {
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
