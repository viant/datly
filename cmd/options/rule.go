package options

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	"github.com/viant/datly/internal/setter"
	"os"
)

type Rule struct {
	Project string `short:"p" long:"proj" description:"project location"`
	Name    string `short:"n" long:"name" description:"rule name"`
	Prefix  string `short:"u" long:"uri" description:"rule uri"  default:"dev" `
	Source  string `short:"s" long:"src" description:"source"`
	Module  string `short:"m" long:"module" description:"go module package root" default:"pkg"`
}

func (g *Rule) Init() error {
	if g.Project == "" {
		g.Project, _ = os.Getwd()
	}
	setter.SetStringIfEmpty(&g.Prefix, "dev")
	g.Project = ensureAbsPath(g.Project)
	if url.IsRelative(g.Module) {
		g.Module = url.Join(g.Project, g.Module)
	}
	expandRelativeIfNeeded(&g.Source, g.Project)
	return nil
}

func (g *Rule) LoadSource(ctx context.Context, fs afs.Service) (string, error) {
	data, err := fs.DownloadWithURL(ctx, g.Source)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
