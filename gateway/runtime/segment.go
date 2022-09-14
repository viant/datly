package runtime

import (
	nurl "net/url"
	"strings"
)

func RemoveFirstSegment(path string) string {
	notSlashOccured := false
	nextSegment := strings.IndexFunc(path, func(r rune) bool {
		if r == '/' {
			if notSlashOccured {
				return true
			}
		} else {
			notSlashOccured = true
		}

		return false
	})

	if nextSegment == -1 {
		return path
	}

	return path[nextSegment:]
}

func RemoveFirstURLSegment(URL *nurl.URL) (*nurl.URL, error) {
	URI := URL.String()

	pathStart := strings.Index(URI, URL.Path)
	if pathStart < 0 {
		pathStart = 0
	}

	withoutFirstSeg := RemoveFirstSegment(URL.Path)

	actualURL := URI[:pathStart] + strings.Replace(URI[pathStart:], URL.Path, withoutFirstSeg, 1)
	return nurl.Parse(actualURL)
}
