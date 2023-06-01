package options

import (
	"github.com/viant/afs/url"
	"os"
)

type Generate struct {
	Project string `short:"p" long:"proj" description:"project location"`
	Name    string `short:"n" long:"name" description:"rule name"`
	Source  string `short:"s" long:"src" description:"source"`
	Module  string `short:"m" long:"module" description:"go module package root" default:"pkg"`
}

func (g *Generate) Init() error {
	if g.Project == "" {
		g.Project, _ = os.Getwd()
	}
	g.Project = ensureAbsPath(g.Project)
	if url.IsRelative(g.Module) {
		g.Module = url.Join(g.Project, g.Module)
	}
	expandRelativeIfNeeded(&g.Source, g.Project)
	return nil
}
