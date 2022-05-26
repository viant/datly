package cmd

import (
	"github.com/viant/afs/url"
	"os"
	"path"
	"strings"
)

func normalizeURL(loc string) string {
	if strings.HasSuffix(loc, "/") {
		return loc
	}
	if url.Scheme(loc, "e") != "e" {
		return loc
	}
	baseDir, _ := os.Getwd()
	return path.Join(baseDir, loc)
}
