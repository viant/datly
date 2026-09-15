package dql

import (
	"strings"
	"testing"
)

func TestFilenameSettings(t *testing.T) {
	source := `#setting($_ = $file_prefix('orders_'))
#setting($_ = $handler_dest('execute.go'))
#setting($_ = $lifecycle_dest('custom.go'))
#setting($_ = $mutation_dest('policy.go'))
#setting($_ = $resources_dest('sql.go'))
#setting($_ = $links_dest('register.go'))
#setting($_ = $template_dest('templates/main.velty'))
#setting($_ = $support_dest('frames','state.go'))
#setting($_ = $support_dest('type:CubeInput','cube.go'))
SELECT 1`
	prepared := PrepareSource(source)
	if err := prepared.Err(); err != nil {
		t.Fatal(err)
	}
	settings := prepared.Directives.Settings.Generation
	for role, expected := range map[string]string{"handler": "execute.go", "lifecycle": "custom.go", "mutation": "policy.go", "resources": "sql.go", "links": "register.go", "template": "templates/main.velty", "frames": "state.go", "type:CubeInput": "cube.go"} {
		if actual := settings.File(role, "default.go"); actual != expected {
			t.Fatalf("%s: %s", role, actual)
		}
	}
	if settings.File("input", "input.go") != "orders_input.go" {
		t.Fatal("missing prefix")
	}
	clone := settings.Clone()
	clone.SupportFiles["frames"] = "different.go"
	if settings.SupportFiles["frames"] != "state.go" {
		t.Fatal("aliased support files")
	}
}

func TestFilenameSettingsRejectMalformedDirectives(t *testing.T) {
	for _, directive := range []string{
		"$file_prefix('../escape_')", "$file_prefix('/tmp/')", "$file_prefix('a/b_')", "$file_prefix('a\\b_')", "$file_prefix('_hidden_')", "$file_prefix(' a_')",
		"$handler_dest()", "$lifecycle_dest('')", "$mutation_dest('a.go','b.go')", "$resources_dest(123)", "$links_dest('a.go').Extra()",
		"$support_dest('unknown','a.go')", "$support_dest('type:bad','a.go')", "$support_dest('frames','')",
		"$handler_dest('a.go'))\n#setting($_ = $handler_dest('b.go')", "$support_dest('frames','a.go'))\n#setting($_ = $support_dest('frames','b.go')",
	} {
		t.Run(directive, func(t *testing.T) {
			err := PrepareSource("#setting($_ = " + directive + ")\nSELECT 1").Err()
			if err == nil {
				t.Fatal("invalid filename control accepted", strings.TrimSpace(directive))
			}
		})
	}
}
