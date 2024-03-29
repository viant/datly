package options

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/internal/translator/parser"
	"github.com/viant/parsly"
	"os"
	"path"
	"strings"
)

type Rule struct {
	Project        string   `short:"p" long:"proj" description:"project location"`
	Name           string   `short:"n" long:"name" description:"rule name"`
	ModulePrefix   string   `short:"u" long:"namespace" description:"rule uri/namespace"  default:"dev" `
	Source         []string `short:"s" long:"src" description:"source"`
	Packages       []string `short:"g" long:"pkg" description:"entity package"`
	Output         []string
	Index          int
	ModuleLocation string `short:"m" long:"module" description:"go module package root" default:"pkg"`
	Generated      bool
}

func (r *Rule) SourceCodeLocation() string {
	fileFolder := r.GoModuleLocation()
	if r.ModulePrefix != "" {
		fileFolder = url.Join(fileFolder, r.ModulePrefix)
	}
	return fileFolder
}

func (r *Rule) GoModuleLocation() string {
	if r.ModuleLocation != "" {
		return r.ModuleLocation
	}
	return "pkg"
}

func (r *Rule) ModFileLocation(ctx context.Context) string {
	goMod := path.Join(r.ModuleLocation, "go.mod")
	if ok, _ := fs.Exists(ctx, goMod); ok {
		return r.ModuleLocation
	}
	parent, _ := path.Split(r.ModuleLocation)
	if ok, _ := fs.Exists(ctx, path.Join(parent, "go.mod")); ok {
		return parent
	}
	return r.ModuleLocation
}

func (r *Rule) BaseRuleURL() string {
	return url.Path(url.Join(r.Project, "dql"))
}

func (r *Rule) GoCodeLocation() string {
	module := r.GoModuleLocation()
	if r.Package() == "" {
		return module
	}
	if r.ModulePrefix != "" {
		if strings.HasSuffix(r.ModulePrefix, r.Package()) {
			return url.Join(module, r.ModulePrefix)
		}
		return url.Join(module, r.ModulePrefix, r.Package())
	}
	return url.Join(module, r.Package())
}

func (r *Rule) Package() string {
	if r.Index < len(r.Packages) {
		return r.Packages[r.Index]
	}
	pkg := extractPackageFromSource(r.SourceURL())
	if pkg != "dql" {
		return pkg
	}
	return ""
}

func extractPackageFromSource(sourceURL string) string {
	baseURL, _ := url.Split(sourceURL, file.Scheme)
	_, pkg := url.Split(baseURL, file.Scheme)
	builder := strings.Builder{}
	hasLeter := false
	for i := 0; i < len(pkg); i++ {
		ch := pkg[i]
		switch ch {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '_':
			if hasLeter {
				builder.WriteByte(ch)
			}
		default:
			hasLeter = true
			builder.WriteByte(ch)
		}
	}
	return builder.String()
}

func (r *Rule) RuleName() string {
	URL := r.SourceURL()
	_, name := url.Split(URL, file.Scheme)
	if ext := path.Ext(name); ext != "" {
		name = name[:len(name)-len(ext)]
	}
	return name
}

func (r *Rule) SourceDirectory() string {
	URL := r.SourceURL()
	baseURL, _ := url.Split(URL, file.Scheme)
	return url.Path(baseURL)
}

func (r *Rule) Init() error {
	if r.Project == "" {
		r.Project, _ = os.Getwd()
	}
	setter.SetStringIfEmpty(&r.ModulePrefix, "dev")
	r.Project = ensureAbsPath(r.Project)
	if url.IsRelative(r.ModuleLocation) {
		r.ModuleLocation = url.Join(r.Project, r.ModuleLocation)
	}
	r.expandSourceIfNeeded()
	for i := range r.Source {
		expandRelativeIfNeeded(&r.Source[i], r.Project)
	}
	if r.Index == 0 && len(r.Source) == 1 {
		src := r.Source[r.Index]
		object, err := fs.Object(context.Background(), src)
		if err != nil {
			return fmt.Errorf("failed to locate source: %s, %w", src, err)
		}
		if object.IsDir() {
			r.expandFolderSource(src)
		}
	}

	return nil
}

func (r *Rule) expandFolderSource(src string) {
	var sourceURLs []string
	if objects, _ := fs.List(context.Background(), src); len(objects) > 0 {
		for _, candidate := range objects {
			if candidate.IsDir() {
				continue
			}
			if path.Ext(candidate.Name()) == ".sql" {
				sourceURLs = append(sourceURLs, candidate.URL())
			}
		}
	}
	r.Source = sourceURLs
}

func (r *Rule) SourceURL() string {
	if len(r.Source) == 0 {
		return ""
	}
	if r.Index >= len(r.Source) {
		return r.Source[0]
	}
	return r.Source[r.Index]
}

func (r *Rule) LoadSource(ctx context.Context, fs afs.Service, URL string) (string, error) {
	data, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (r *Rule) expandSourceIfNeeded() {
	var expanded []string
	useExpanded := false
	for _, item := range r.Source {
		parent, name := path.Split(item)
		names := strings.Split(name, "|")
		if len(names) > 1 {
			useExpanded = true
			for _, fname := range names {
				expanded = append(expanded, path.Join(parent, strings.TrimSpace(fname)))
			}
			continue
		}
		expanded = append(expanded, item)
	}
	if !useExpanded {
		return
	}
	r.Source = expanded
}

func (r *Rule) NormalizeComponent(dSQL *string) {
	if index := strings.Index(*dSQL, parser.ComponentKeywordMatcher.Name); index != -1 {
		cursor := parsly.NewCursor("", []byte((*dSQL)[index+len(parser.ComponentKeywordMatcher.Name):]), 0)
		if match := cursor.MatchOne(parser.ParenthesesBlockMatcher); match.Code == parser.ParenthesesBlockToken {
			text := match.Text(cursor)
			fromText := parser.ComponentKeywordMatcher.Name + text
			JSON := "{" + text[1:len(text)-1] + "}"
			aRule := struct {
				Package string
				Name    string
			}{}
			if err := json.Unmarshal([]byte(JSON), &aRule); err == nil {
				if aRule.Package != "" {
					r.ModulePrefix = aRule.Package
					r.Name = aRule.Name
				}
			}
			toText := `/* ` + JSON + ` */`
			*dSQL = strings.Replace(*dSQL, fromText, toText, 1)
		}
	}
}
