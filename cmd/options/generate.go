package options

import (
	"os"
)

type Generate struct {
	Project string `short:"p" long:"proj" description:"project location"`
	Name    string `short:"n" long:"name" description:"rule name"`
	Source  string `short:"s" long:"src" description:"source"`
	Dest    string `short:"d" long:"dest" description:"dsql location"`
	Module  string `short:"m" long:"module" description:"go module package root"`
	Package string `short:"g" long:"pkg" description:"entity package"`
}

func (g *Generate) Init() error {
	if g.Project == "" {
		g.Project, _ = os.Getwd()
	}
	if g.Module == "" {
		g.Module = "pkg"
	}
	g.Module = ensureAbsPath(g.Module)
	return nil
}
