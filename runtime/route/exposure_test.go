package route

import "testing"

func TestPackageExposurePatterns(t *testing.T) {
	for _, tt := range []struct {
		name             string
		include, exclude []string
		pkg              string
		want             bool
	}{
		{"exact", []string{"example.org/a/api"}, nil, "example.org/a/api", true},
		{"other module", []string{"example.net/b/..."}, nil, "example.net/b/api", true},
		{"subtree root", []string{"example.net/b/..."}, nil, "example.net/b", true},
		{"prefix boundary", []string{"example.net/b/..."}, nil, "example.net/bad/api", false},
		{"exclude wins", []string{"..."}, []string{"example.net/b/..."}, "example.net/b/private", false},
		{"empty hides all", nil, nil, "example.org/a", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			policy, err := NewExposure(tt.include, tt.exclude)
			if err != nil {
				t.Fatal(err)
			}
			if policy.Allows(tt.pkg) != tt.want {
				t.Fatalf("unexpected selection of %s", tt.pkg)
			}
		})
	}
	for _, pattern := range []string{"", "["} {
		if _, err := NewExposure([]string{pattern}, nil); err == nil {
			t.Fatalf("invalid pattern accepted: %q", pattern)
		}
	}
	include := []string{"example.org/api"}
	policy, err := NewExposure(include, nil)
	if err != nil {
		t.Fatal(err)
	}
	include[0] = "..."
	if policy.Allows("example.net/private") {
		t.Fatal("exposure aliases caller slice")
	}
}
