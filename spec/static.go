package spec

import (
	"fmt"
	"io/fs"
	"path"
	"strings"
)

// StaticContent maps a resource subtree to one literal HTTP prefix. Namespace
// selects the shared resource.Store. ContentURL selects operator AFS authority.
// Exactly one source is required; Root is relative to that source.
type StaticContent struct {
	Path         string `json:"path"`
	Namespace    string `json:"namespace,omitempty"`
	ContentURL   string `json:"contentURL,omitempty"`
	Root         string `json:"root,omitempty"`
	CORS         *CORS  `json:"cors,omitempty"`
	APIKeyHeader string `json:"apiKeyHeader,omitempty"`
	APIKeyValue  string `json:"apiKeyValue,omitempty"`
}

func (s *StaticContent) Clone() *StaticContent {
	if s == nil {
		return nil
	}
	c := *s
	c.CORS = s.CORS.Resolve(nil)
	return &c
}
func (s *StaticContent) Validate() error {
	if s == nil {
		return fmt.Errorf("static content is required")
	}
	prefix := strings.TrimSuffix(s.Path, "/")
	if s.Path != "/" && (prefix == "" || prefix == "/" || !strings.HasPrefix(prefix, "/") || path.Clean(prefix) != prefix) || strings.ContainsAny(s.Path, "{}*%\\?#\r\n") {
		return fmt.Errorf("static path must be a literal absolute URL prefix")
	}
	if (s.Namespace == "") == (s.ContentURL == "") {
		return fmt.Errorf("static content requires exactly one Namespace or ContentURL")
	}
	if strings.ContainsAny(s.Namespace, ":/\\") || strings.TrimSpace(s.Namespace) != s.Namespace {
		return fmt.Errorf("invalid static namespace")
	}
	if s.Root != "" && (!fs.ValidPath(s.Root) || strings.ContainsAny(s.Root, "\\%?#")) {
		return fmt.Errorf("static root must be a relative filesystem directory")
	}
	return nil
}
