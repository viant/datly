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
	if strings.Contains(location, "~") {
		location = strings.Replace(location, "~", os.Getenv("HOME"), 1)
	}
	return location
}

func expandRelativeIfNeeded(location *string, projectRoot string) {
	if !url.IsRelative(*location) {
		return
	}

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
