package cmd

import (
	"github.com/viant/afs/url"
	"os"
	"path"
	"strings"
)

func normalizeURL(loc string) string {
	if strings.HasPrefix(loc, "/") {
		return loc
	}

	if scheme := url.Scheme(loc, "e"); scheme == "e" {
		baseDir, _ := os.Getwd()
		return path.Join(baseDir, loc)
	}

	return loc
}
