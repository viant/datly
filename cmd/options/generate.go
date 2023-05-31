package options

import (
	"os"
)

type Generate struct {
	Project string `short:"p" long:"proj" description:"project location"`
	Name    string `short:"n" long:"name" description:"rule name"`
	Source  string `short:"s" long:"src" description:"source"`
	Module  string `short:"m" long:"module" description:"go module package root" default:"pkg"`
	Package string `short:"g" long:"pkg" description:"entity package"`
}

func (g *Generate) Init() error {
	if g.Project == "" {
		g.Project, _ = os.Getwd()
	}
	g.Project = ensureAbsPath(g.Project)
	if g.Module == "" {
		g.Module = "pkg"
	}
	expandRelativeIfNeeded(&g.Module, g.Project)
	expandRelativeIfNeeded(&g.Source, g.Project)
	return nil
}
