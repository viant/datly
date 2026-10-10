package developer

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
)

func TestTranscribeSelectsSourceWithinOriginalPackage(t *testing.T) {
	for _, selected := range []string{"users.dql", "accounts.dql", "missing.dql", "", "ambiguous"} {
		t.Run(selected, func(t *testing.T) {
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "example.com/app"}).Write(t, root)
			if err := os.MkdirAll(filepath.Join(root, "source"), 0755); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"users", "accounts"} {
				source := fmt.Sprintf(`#package('example.com/app/api/%s')
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#setting($_ = $route('/%s','GET'))
#define($_ = $Data<[]*Row>(output/view))
SELECT r.*, type(r,'Row'), CAST(r.ID AS int)
FROM (SELECT 1 AS ID) r
`, name, name)
				if err := os.WriteFile(filepath.Join(root, "source", name+".dql"), []byte(source), 0600); err != nil {
					t.Fatal(err)
				}
			}
			pattern := "example.com/app/source"
			selection := selected
			if selected == "ambiguous" {
				selection = "users.dql"
				pattern += "/..."
				nested := filepath.Join(root, "source", "nested")
				if err := os.MkdirAll(nested, 0755); err != nil {
					t.Fatal(err)
				}
				original, err := os.ReadFile(filepath.Join(root, "source", "users.dql"))
				if err != nil {
					t.Fatal(err)
				}
				duplicate := strings.ReplaceAll(string(original), "api/users", "api/otherusers")
				duplicate = strings.ReplaceAll(duplicate, "'/users'", "'/otherusers'")
				if err := os.WriteFile(filepath.Join(nested, "users.dql"), []byte(duplicate), 0600); err != nil {
					t.Fatal(err)
				}
			}
			args := []string{"transcribe", "get", "-dir", root}
			if selection != "" {
				args = append(args, "-source", selection)
			}
			args = append(args, pattern)
			var out, diagnostic bytes.Buffer
			status := Run(context.Background(), args, &out, &diagnostic)
			accepted := selected == "users.dql" || selected == "accounts.dql"
			if accepted {
				if status != 0 {
					t.Fatalf("status=%d diagnostic=%s", status, diagnostic.String())
				}
				target := strings.TrimSuffix(selected, ".dql")
				if files, err := os.ReadDir(filepath.Join(root, "api", target)); err != nil || len(files) == 0 {
					t.Fatalf("selected output missing: %v", err)
				}
				other := "users"
				if target == other {
					other = "accounts"
				}
				if _, err := os.Stat(filepath.Join(root, "api", other)); !os.IsNotExist(err) {
					t.Fatalf("unselected component emitted: %v", err)
				}
			} else {
				if status == 0 || !strings.Contains(diagnostic.String(), "exactly one component") {
					t.Fatalf("ambiguous/missing source accepted: %d %s", status, diagnostic.String())
				}
				if _, err := os.Stat(filepath.Join(root, "api")); !os.IsNotExist(err) {
					t.Fatalf("failed selection emitted output: %v", err)
				}
			}
		})
	}
}
