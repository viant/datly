package developer

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
)

func TestTranscribeBatchCLI(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		want int
	}{
		{"directory", []string{"-batch", "source"}, 2},
		{"explicit", []string{"source/b.dql", "source/a.dql"}, 2},
		{"single file", []string{"-batch", "source/a.dql"}, 1},
		{"exclude", []string{"-batch", "-exclude", "example.com/batch/ignored/...", "example.com/batch/..."}, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "example.com/batch"}).Write(t, root)
			require.NoError(t, os.Mkdir(filepath.Join(root, "source"), 0755))
			require.NoError(t, os.Mkdir(filepath.Join(root, "ignored"), 0755))
			require.NoError(t, os.WriteFile(filepath.Join(root, "ignored/broken.dql"), []byte("broken"), 0600))
			for _, name := range []string{"a", "b"} {
				source := fmt.Sprintf(`#package('example.com/batch/api/%s')
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#setting($_ = $route('/%s','GET'))
#define($_ = $Data<[]*Row>(output/view))
SELECT r.*, type(r,'Row'), CAST(r.ID AS int) FROM (SELECT 1 AS ID) r`, name, name)
				require.NoError(t, os.WriteFile(filepath.Join(root, "source", name+".dql"), []byte(source), 0600))
			}
			var out, diagnostic bytes.Buffer
			status := Run(t.Context(), append([]string{"transcribe", "get", "-dir", root}, test.args...), &out, &diagnostic)
			require.Equal(t, 0, status, diagnostic.String())
			require.Equal(t, test.want, bytes.Count(out.Bytes(), []byte("Generated go get")))
		})
	}
}
