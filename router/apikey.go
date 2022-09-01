package router

import (
	"strings"
)

type (
	APIKey struct {
		URI    string
		Value  string
		Header string
	}

	APIKeys []*APIKey
)

func (a APIKeys) Match(URI string) *APIKey {
	//a needs to be sorted by longest URI
	for _, candidate := range a {
		if candidate.URI == "" || strings.HasPrefix(URI, candidate.URI) {
			return candidate
		}
	}
	return nil
}

func (a APIKeys) Len() int           { return len(a) }
func (a APIKeys) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a APIKeys) Less(i, j int) bool { return len(a[i].URI) > len(a[j].URI) }
