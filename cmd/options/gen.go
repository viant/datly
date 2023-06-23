package options

import (
	"fmt"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"path"
	"strings"
)

type Gen struct {
	Connector
	Generate
	Package   string `short:"g" long:"pkg" description:"entity package"`
	Dest      string `short:"d" long:"dest" description:"dsql location" default:"dsql"`
	Operation string `short:"o" long:"op" description:"operation" choice:"post" choice:"patch" choice:"put"`
	Kind      string `short:"k" long:"kind" description:"execution kind" choice:"dml" choice:"service"`
	Lang      string `short:"l" long:"lang" description:"lang" choice:"go" choice:"velty"`
}

func (g *Gen) GoModuleLocation() string {
	if g.Module != "" {
		return g.Module
	}
	return g.Dest
}

func (g *Gen) GoCodeLocation() string {
	module := g.GoModuleLocation()
	if g.Package == "" {
		return module
	}
	return url.Join(module, g.Package)
}

func (g *Gen) EntityLocation(entityName string) string {
	codeLocation := g.GoCodeLocation()

	entityName = strings.ToLower(entityName)
	return url.Join(codeLocation, entityName+".go")
}

func (g *Gen) StateLocation() string {
	codeLocation := g.GoCodeLocation()
	return url.Join(codeLocation, "state.go")
}

func (g *Gen) Init() error {
	if err := g.Generate.Init(); err != nil {
		return err
	}
	if g.Operation == "" {
		return fmt.Errorf("operation was empty")
	}
	if g.Dest == "" {
		g.Dest = "dsql"
	}
	if url.IsRelative(g.Dest) {
		g.Dest = url.Join(g.Project, g.Dest)
	}
	return nil
}

func (g *Gen) DSQLLocation() string {
	_, name := url.Split(g.Source, file.Scheme)
	if ext := path.Ext(name); ext != "" {
		name = name[:len(name)-len(ext)]
	}
	baseURL := g.Dest
	if g.Package != "" {
		baseURL = url.Join(baseURL, g.Package)
	}
	return url.Join(baseURL, name+".sql")
}
