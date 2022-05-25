package cmd

import (
	"github.com/viant/afs/url"
	"os"
	"path"
)

func normalizeURL(loc string) string {
	if url.Scheme(loc, "e") != "e" {
		return loc
	}
	baseDir, _ := os.Getwd()
	return path.Join(baseDir, loc)
}
