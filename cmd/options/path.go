package options

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	"os"
	"strings"
)

var fs = afs.New()

func ensureAbsPath(location string) string {
	location = expandHomeDir(location)
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

func expandHomeDir(location string) string {
	return strings.Replace(location, "~", os.Getenv("HOME"), 1)
}

func expandRelativeIfNeeded(location *string, projectRoot string) {
	if location == nil || *location == "" {
		return
	}
	if !url.IsRelative(*location) {
		return
	}
	*location = expandHomeDir(*location)

	//check relative first
	if wd, _ := os.Getwd(); wd != "" {
		candidate := url.Join(wd, *location)
		if ok, _ := fs.Exists(context.Background(), candidate); ok || projectRoot == "" {
			*location = candidate
			return
		}
	}
	loc := url.Join(projectRoot, *location)
	*location = loc
}
