package spec

import (
	"fmt"
	"io/fs"
	"net/url"
	"path"
	"strings"
)

// ResourceFolder is target-neutral authored MCP publication metadata.
type ResourceFolder struct {
	Skills    []string `json:"skills,omitempty"`
	Namespace string   `json:"namespace"`
	Root      string   `json:"root"`
	URIPrefix string   `json:"uriPrefix"`
}

func (f ResourceFolder) Clone() ResourceFolder {
	f.Skills = append([]string(nil), f.Skills...)
	return f
}

func (f ResourceFolder) Validate() error {
	seen := map[string]bool{}
	for _, root := range f.Skills {
		if !fs.ValidPath(root) || strings.Contains(root, "\\") || seen[root] {
			return fmt.Errorf("invalid or duplicate skill root")
		}
		seen[root] = true
	}
	if f.Namespace == "" || strings.TrimSpace(f.Namespace) != f.Namespace || strings.ContainsAny(f.Namespace, ":/\\") {
		return fmt.Errorf("resource namespace is required")
	}
	if f.Root != "" && (!fs.ValidPath(f.Root) || strings.Contains(f.Root, "\\")) {
		return fmt.Errorf("invalid resource root")
	}
	u, err := url.Parse(f.URIPrefix)
	if err != nil {
		return fmt.Errorf("invalid resource URI prefix")
	}
	base := strings.TrimSuffix(u.Path, "/")
	if u.Scheme == "" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || u.RawPath != "" || strings.ContainsAny(u.Path, "{}%\\") || base != "" && path.Clean(base) != base {
		return fmt.Errorf("invalid resource URI prefix")
	}
	return nil
}
