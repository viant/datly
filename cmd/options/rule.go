package options

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/internal/setter"
	"os"
	"path"
	"strings"
)

type Rule struct {
	Project      string   `short:"p" long:"proj" description:"project location"`
	Name         string   `short:"n" long:"name" description:"rule name"`
	Prefix       string   `short:"u" long:"uri" description:"rule uri"  default:"dev" `
	Source       []string `short:"s" long:"src" description:"source"`
	Packages     []string `short:"g" long:"pkg" description:"entity package"`
	Output       []string
	Index        int
	CustomRouter string `short:"R" long:"router" description:"custom router location"`
	Module       string `short:"m" long:"module" description:"go module package root" default:"pkg"`
	Generated    bool
}

func (r *Rule) GoModuleLocation() string {
	if r.Module != "" {
		return r.Module
	}
	return "pkg"
}

func (r *Rule) BaseRuleURL() string {
	return url.Path(url.Join(r.Project, "dsql"))
}

func (r *Rule) GoCodeLocation() string {
	module := r.GoModuleLocation()
	if r.Package() == "" {
		return module
	}
	return url.Join(module, r.Package())
}

func (r *Rule) Package() string {
	if r.Index < len(r.Packages) {
		return r.Packages[r.Index]
	}
	pkg := extractPackageFromSource(r.SourceURL())
	if pkg != "dsql" {
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
	setter.SetStringIfEmpty(&r.Prefix, "dev")
	r.Project = ensureAbsPath(r.Project)
	if url.IsRelative(r.Module) {
		r.Module = url.Join(r.Project, r.Module)
	}
	expandRelativeIfNeeded(&r.Source[r.Index], r.Project)
	return nil
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
