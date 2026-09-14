package packageasset

import (
	"context"
	"embed"
	"fmt"
	"github.com/viant/afs"
	afsio "github.com/viant/afs/adapter/io"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/spec"
	"io/fs"
	"net/url"
	"os"
	"path"
	"strings"
)

// StaticSource resolves static authority once per generation. Local OS roots are
// confined while snapshotting; remote ContentURL keeps AFS ownership.
type StaticSource struct {
	// LocalRoot is an optional caller-owned authority selected independently of
	// ContentURL. Local ContentURL must be relative to it. The caller must keep it
	// open through staging and must not derive it by following configured paths.
	// Nil uses the volume root for absolute paths, or cwd for relative paths.
	LocalRoot  *os.Root
	Resources  *resource.Store
	ContentURL string
}

func (s StaticSource) Snapshot(ctx context.Context, content *spec.StaticContent) (*embed.FS, error) {
	if err := content.Validate(); err != nil {
		return nil, err
	}
	if content.Namespace != "" {
		source, ok := s.Resources.Lookup(content.Namespace)
		if !ok {
			return nil, fmt.Errorf("static namespace %q is not registered", content.Namespace)
		}
		return (Snapshotter{Source: source}).Folder(ctx, content.Root)
	}
	location := content.ContentURL
	subtree := content.Root
	if s.ContentURL != "" {
		if !fs.ValidPath(location) || strings.ContainsAny(location, ":\\%?#") {
			return nil, fmt.Errorf("route ContentURL must be a relative directory beneath configured ContentURL")
		}
		subtree = path.Join(location, subtree)
		location = s.ContentURL
	}
	parsed, err := url.Parse(location)
	if err != nil {
		return nil, err
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil || parsed.Opaque != "" {
		return nil, fmt.Errorf("invalid static ContentURL")
	}
	if parsed.Scheme == "" || parsed.Scheme == "file" {
		if parsed.Host != "" && parsed.Host != "localhost" {
			return nil, fmt.Errorf("static file URL must be local")
		}
		return s.localSnapshot(ctx, parsed.Path, subtree)
	}
	return (Snapshotter{Source: afsio.NewFS(ctx, afs.New(), location)}).Folder(ctx, subtree)
}
