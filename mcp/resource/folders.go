package resource

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io/fs"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"

	bindresource "github.com/viant/bindly/resource"
	"github.com/viant/datly/internal/packageasset"
	"github.com/viant/datly/spec"
	skillformat "github.com/viant/mcp-protocol/extension/skills"
	"github.com/viant/mcp-protocol/schema"
)

// Folder explicitly publishes one complete, immutable tree. Namespace groups
// metadata and selects a canonical Store source when FS is absent. LocalDir is
// an optional development override, confined by os.Root; binaries should use FS.
type Folder struct {
	// Skills explicitly declares relative skill directory roots ("." for this
	// folder). Descendant SKILL.md files are supporting files unless declared.
	Skills      []string
	Namespace   string
	Root        string
	URIPrefix   string
	Description string
	FS          fs.FS `json:"-"`
	LocalDir    string
}

type Publisher struct{ Resources *bindresource.Store }

type fileResource struct {
	source fs.FS
	name   string
}

func (p Publisher) Compile(ctx context.Context, folders []Folder) ([]*Plan, error) {
	if ctx == nil {
		return nil, fmt.Errorf("resource publication context is required")
	}
	var result []*Plan
	for _, folder := range folders {
		plans, err := p.folder(ctx, folder)
		if err != nil {
			return nil, err
		}
		result = append(result, plans...)
		if len(result) > 4096 {
			return nil, fmt.Errorf("too many published resource files")
		}
	}
	return result, nil
}

func (p Publisher) folder(ctx context.Context, f Folder) ([]*Plan, error) {
	if err := (spec.ResourceFolder{Namespace: f.Namespace, Root: f.Root, URIPrefix: f.URIPrefix}).Validate(); err != nil {
		return nil, err
	}
	u, _ := url.Parse(f.URIPrefix)
	if f.Root == "" {
		f.Root = "."
	}
	if !fs.ValidPath(f.Root) || strings.Contains(f.Root, "\\") {
		return nil, fmt.Errorf("invalid published root")
	}
	var source fs.FS
	if f.LocalDir != "" {
		if f.FS != nil {
			return nil, fmt.Errorf("resource folder has conflicting sources")
		}
		root, err := os.OpenRoot(f.LocalDir)
		if err != nil {
			return nil, err
		}
		defer root.Close()
		source = root.FS()
	} else if f.FS != nil {
		source = f.FS
	} else {
		var found bool
		source, found = p.Resources.Lookup(f.Namespace)
		if !found {
			return nil, fmt.Errorf("published resource namespace %q is not registered", f.Namespace)
		}
	}
	snapshot, err := (packageasset.Snapshotter{Source: source}).Folder(ctx, f.Root)
	if err != nil {
		return nil, err
	}
	var plans []*Plan
	err = fs.WalkDir(snapshot, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := fs.ReadFile(snapshot, name)
		if err != nil {
			return err
		}
		uri := *u
		uri.Path = strings.TrimSuffix(u.Path, "/") + "/" + name
		uri.RawPath = ""
		kind := mime.TypeByExtension(path.Ext(name))
		switch strings.ToLower(path.Ext(name)) {
		case ".md":
			kind = "text/markdown"
		case ".yaml", ".yml":
			kind = "text/yaml"
		case ".ebnf", ".txt":
			kind = "text/plain"
		case ".json":
			kind = "application/json"
		case ".sql":
			kind = "text/plain"
		}
		if kind == "" {
			kind = http.DetectContentType(data)
		}
		size := len(data)
		description := f.Description
		metadata := &schema.Resource{Uri: uri.String(), Name: f.Namespace + ":" + path.Join(f.Root, name), Size: &size, MimeType: &kind, Meta: map[string]interface{}{"datly.sha256": fmt.Sprintf("%x", sha256.Sum256(data))}}
		if description != "" {
			metadata.Description = &description
		}
		plans = append(plans, &Plan{kind: spec.MCPExposureResource, resource: metadata, file: &fileResource{source: snapshot, name: name}})
		return nil
	})
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, root := range f.Skills {
		if !fs.ValidPath(root) || strings.Contains(root, "\\") || seen[root] {
			return nil, fmt.Errorf("invalid or duplicate skill root %q", root)
		}
		seen[root] = true
		var entrypoint *Plan
		for _, plan := range plans {
			if plan.file.name == path.Join(root, "SKILL.md") {
				entrypoint = plan
				break
			}
		}
		if entrypoint == nil {
			return nil, fmt.Errorf("declared skill %q has no SKILL.md", root)
		}
		subtree, err := fs.Sub(snapshot, root)
		if err != nil {
			return nil, err
		}
		entry, err := (skillformat.Compiler{Source: subtree}).Compile(ctx, entrypoint.URI())
		if err != nil {
			return nil, fmt.Errorf("declared skill %q: %w", root, err)
		}
		entrypoint.skill = entry
		metadata := entry.Metadata()
		title, description := metadata.Frontmatter["name"].(string), metadata.Frontmatter["description"].(string)
		entrypoint.resource.Title, entrypoint.resource.Description = &title, &description
	}
	return plans, nil
}
