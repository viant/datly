package generate

import (
	"context"
	"crypto/sha256"
	"fmt"
	docs "github.com/viant/datly/documentation"
	"io/fs"
	"sort"
	"strconv"
	"strings"

	"github.com/viant/datly/internal/packageasset"
	"github.com/viant/datly/tag"
	"github.com/viant/tagly/tags"
)

// ResourcePlan keeps regenerable package assets separate from append-only Go
// field metadata. The emitted FS is registered explicitly with Bindly.
type ResourcePlan struct {
	Namespace   string
	Destination string
	Symbol      string
	Files       []EmittedFile
	sourceText  string
}

// ResourceManifest is persisted beside generated package ownership metadata.
// Paths are relative to the generated package directory.
type ResourceManifest = packageasset.Resources

func (r *ResourcePlan) retained(file string) bool {
	if r == nil || r.sourceText == "" {
		return false
	}
	if file == r.Destination {
		return true
	}
	for _, asset := range r.Files {
		if file == asset.Path {
			return true
		}
	}
	return false
}

func (r *planResolver) prepareResources() (*ResourcePlan, error) {
	if !r.input.SQLResources && r.plan.Static == nil && r.plan.Documentation.IsZero() && len(r.plan.Settings.MCPFolders) == 0 {
		return nil, nil
	}
	identity := strings.TrimSpace(r.input.TargetPackage) + ":" + r.input.Component.Name
	if strings.TrimSpace(r.input.TargetPackage) == "" {
		return nil, fmt.Errorf("package resource generation requires a target package")
	}
	result := &ResourcePlan{Namespace: fmt.Sprintf("datly_%x", sha256.Sum256([]byte(identity))), Destination: r.plan.Generation.File("resources", "resources.go")}
	if r.input.PackageName != "" {
		result.Symbol = upperCamel(r.input.Component.Name)
	}
	files := map[string]string{}
	if !r.plan.Documentation.IsZero() {
		packaged, err := (docs.Loader{Resources: r.input.Resources}).Package(context.Background(), docs.PackageRequest{Source: r.plan.Documentation, Namespace: result.Namespace})
		if err != nil {
			return nil, err
		}
		r.plan.Documentation = packaged.Source
		for _, asset := range packaged.Assets {
			if existing, ok := files[asset.Path]; ok && existing != string(asset.Data) {
				return nil, fmt.Errorf("conflicting documentation resource %s", asset.Path)
			}
			files[asset.Path] = string(asset.Data)
		}
	}
	for index, folder := range r.plan.Settings.MCPFolders {
		if err := folder.Validate(); err != nil {
			return nil, err
		}
		source, ok := r.input.Resources.Lookup(folder.Namespace)
		if !ok {
			return nil, fmt.Errorf("MCP folder namespace %q is not configured", folder.Namespace)
		}
		snapshot, err := (packageasset.Snapshotter{Source: source}).Folder(context.Background(), folder.Root)
		if err != nil {
			return nil, err
		}
		root := fmt.Sprintf("datly_assets/%d", index)
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
			files[root+"/"+name] = string(data)
			return nil
		})
		if err != nil {
			return nil, err
		}
		r.plan.Settings.MCPFolders[index].Namespace = result.Namespace
		r.plan.Settings.MCPFolders[index].Root = root
	}
	if r.plan.Static != nil {
		if err := r.packageStatic(result, files); err != nil {
			return nil, err
		}
	}
	externalize := func(owner string, fields []Field) error {
		for i := range fields {
			field := &fields[i]
			raw := tags.NewTags(field.Tag).Lookup(tag.SQLName)
			if raw == nil {
				continue
			}
			source := tag.ParseSQL(string(raw.Values))
			if source == nil || strings.TrimSpace(source.Text) == "" {
				continue
			}
			identity := owner + "." + field.Name
			path := fmt.Sprintf("datly_sql/%x.sql", sha256.Sum256([]byte(identity)))
			if existing, ok := files[path]; ok && existing != source.Text {
				return fmt.Errorf("SQL field identity %s has conflicting sources", identity)
			}
			files[path] = source.Text
			field.Tag = appendStructTag(withoutStructTags(field.Tag, tag.SQLName), tag.SQLName, (tag.SQL{URI: result.Namespace + ":" + path}).Value())
		}
		return nil
	}
	if r.input.SQLResources && r.plan.Input.Ownership == ContractGenerated {
		if err := externalize(r.plan.Input.Type, r.plan.Input.Fields); err != nil {
			return nil, err
		}
	}
	if r.input.SQLResources && r.plan.Output.Ownership == ContractGenerated {
		if err := externalize(r.plan.Output.Type, r.plan.Output.Fields); err != nil {
			return nil, err
		}
	}
	for i := range r.plan.Views {
		view := &r.plan.Views[i]
		if r.input.SQLResources && view.Ownership == ViewGenerated {
			if err := externalize(view.Name, view.Fields); err != nil {
				return nil, err
			}
		}
	}
	for path, content := range files {
		result.Files = append(result.Files, EmittedFile{Path: path, Content: content})
	}
	sort.Slice(result.Files, func(i, j int) bool { return result.Files[i].Path < result.Files[j].Path })
	if len(result.Files) == 0 {
		return r.linkedResources(result)
	}
	return result, nil
}

func (r *ResourcePlan) source(packageName string) string {
	if r.sourceText != "" {
		return r.sourceText
	}
	var source strings.Builder
	source.WriteString("package " + packageName + "\n\nimport \"embed\"\n\n")
	source.WriteString("// DatlyResourceNamespace identifies this package's generated resource filesystem.\nconst " + r.Symbol + "DatlyResourceNamespace = " + strconv.Quote(r.Namespace) + "\n\n")
	source.WriteString("// DatlyResources must be registered with the shared Bindly resource store.\n//go:embed")
	for _, file := range r.Files {
		source.WriteString(" " + strconv.Quote(file.Path))
	}
	source.WriteString("\nvar " + r.Symbol + "DatlyResources embed.FS\n")
	return source.String()
}
