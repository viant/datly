package packageasset

import (
	"testing"
	"testing/fstest"
)

func TestManifestResourceDescriptor(t *testing.T) {
	for _, test := range []struct {
		name, source string
		found, fail  bool
	}{
		{"missing", "", false, false},
		{"no resources", `{"version":2,"owner":"Records"}`, false, false},
		{"generated", `{"version":2,"resources":{"namespace":"queries","files":["datly_sql/query.sql"]}}`, true, false},
		{"invalid JSON", `{`, false, true},
		{"namespace required", `{"resources":{"files":["query.sql"]}}`, false, true},
		{"qualified namespace", `{"resources":{"namespace":"a:b","files":["query.sql"]}}`, false, true},
		{"empty files", `{"resources":{"namespace":"queries"}}`, false, true},
		{"escaped path", `{"resources":{"namespace":"queries","files":["../query.sql"]}}`, false, true},
		{"absolute path", `{"resources":{"namespace":"queries","files":["/query.sql"]}}`, false, true},
		{"duplicate path", `{"resources":{"namespace":"queries","files":["query.sql","query.sql"]}}`, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			files := fstest.MapFS{}
			if test.source != "" {
				files[ManifestName] = &fstest.MapFile{Data: []byte(test.source)}
			}
			actual, err := Read(files)
			if (err != nil) != test.fail || (actual != nil) != test.found {
				t.Fatalf("manifest=%+v err=%v", actual, err)
			}
		})
	}
}
