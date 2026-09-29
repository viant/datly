package path

import (
	spath "path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/viant/afs/file"
	"github.com/viant/afs/object"
	"github.com/viant/afs/storage"
)

func objectURLs(candidates []storage.Object) []string {
	var result []string
	for _, candidate := range candidates {
		result = append(result, candidate.URL())
	}
	return result
}

func newObject(URL string) storage.Object {
	name := spath.Base(URL)
	return object.New(URL, file.NewInfo(name, 0, file.DefaultFileOsMode, time.Now(), false), nil)
}

func newObjects(URLs ...string) []storage.Object {
	var result []storage.Object
	for _, URL := range URLs {
		result = append(result, newObject(URL))
	}
	return result
}

// The recursive listing behind paths.yaml reflects directory enumeration order,
// so without an explicit sort the same repository produces a different
// paths.yaml on every machine and any partial regeneration reshuffles it.
func TestSortByURL(t *testing.T) {
	testCases := []struct {
		description string
		urls        []string
		expect      []string
	}{
		{
			description: "enumeration order is normalised to lexical order",
			urls: []string{
				"file:///repo/routes/system/session/session.yaml",
				"file:///repo/routes/mdp/adorder/forecast.yaml",
				"file:///repo/routes/platform/agency/agency.yaml",
			},
			expect: []string{
				"file:///repo/routes/mdp/adorder/forecast.yaml",
				"file:///repo/routes/platform/agency/agency.yaml",
				"file:///repo/routes/system/session/session.yaml",
			},
		},
		{
			description: "already sorted stays sorted",
			urls:        []string{"file:///repo/routes/a.yaml", "file:///repo/routes/b.yaml"},
			expect:      []string{"file:///repo/routes/a.yaml", "file:///repo/routes/b.yaml"},
		},
		{
			description: "single entry",
			urls:        []string{"file:///repo/routes/only.yaml"},
			expect:      []string{"file:///repo/routes/only.yaml"},
		},
		{
			description: "empty listing is a no-op",
			urls:        nil,
			expect:      nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			candidates := newObjects(testCase.urls...)
			sortByURL(candidates)
			assert.Equal(t, testCase.expect, objectURLs(candidates))
		})
	}
}

// Sorting must be idempotent, otherwise repeated regeneration still churns.
func TestSortByURL_Idempotent(t *testing.T) {
	candidates := newObjects(
		"file:///repo/routes/z.yaml",
		"file:///repo/routes/a.yaml",
		"file:///repo/routes/m.yaml",
	)
	sortByURL(candidates)
	first := objectURLs(candidates)
	sortByURL(candidates)
	assert.Equal(t, first, objectURLs(candidates))
}
