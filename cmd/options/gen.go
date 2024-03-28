package options

import (
	"fmt"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"path"
	"strings"
)

type Generate struct {
	Repository
	Rule
	Dest      string `short:"d" long:"dest" description:"dql file location" default:"dql"`
	Operation string `short:"o" long:"op" description:"operation" choice:"post" choice:"patch" choice:"put" choice:"get"`
	Kind      string `short:"k" long:"kind" description:"execution kind" choice:"dml" choice:"service"`
	Lang      string `short:"l" long:"lang" description:"lang" choice:"velty" choice:"go"`
	Translate bool   `short:"t" long:"translate" description:"translate generated DSQL"`
}

func (g *Generate) HttpMethod() string {
	return strings.ToUpper(g.Operation)
}

func (g *Generate) EntityLocation(entityName string) string {
	codeLocation := g.GoCodeLocation()

	entityName = strings.ToLower(entityName)
	return url.Join(codeLocation, entityName+".go")
}

func (g *Generate) InputLocation(prefix string, methodFragment string) string {
	codeLocation := g.GoCodeLocation()
	return url.Join(codeLocation, prefix+methodFragment+"input.go")
}

func (g *Generate) OutputLocation(prefix string, methodFragment string) string {
	codeLocation := g.GoCodeLocation()
	return url.Join(codeLocation, prefix+methodFragment+"output.go")
}

func (g *Generate) EmbedLocation(URI string) string {
	codeLocation := g.GoCodeLocation()
	return url.Join(codeLocation, URI)
}

func (g *Generate) Init() error {
	if err := g.Rule.Init(); err != nil {
		return err
	}
	if g.Operation == "" {
		return fmt.Errorf("operation was empty")
	}
	if g.Dest == "" {
		g.Dest = "dql"
	}
	if g.Lang == "" {
		g.Lang = "velty"
	}
	if url.IsRelative(g.Dest) {
		g.Dest = url.Join(g.Project, g.Dest)
	}
	return nil
}

func (g *Generate) DSQLLocation() string {
	_, name := url.Split(g.SourceURL(), file.Scheme)

	pkg := ""
	if len(g.Packages) > 0 {
		pkg = g.Packages[0]
		name = strings.Title(pkg) + "_" + strings.ToLower(g.Operation)
	}

	if ext := path.Ext(name); ext != "" {
		name = name[:len(name)-len(ext)]
	}
	baseURL := g.Dest
	if g.Package() != "" {
		baseURL = url.Join(baseURL, g.Package())
	}
	return url.Join(baseURL, name+".sql")
}

func (g *Generate) HandlerLocation(prefix string, methodFragment string) string {
	_, name := url.Split(g.SourceURL(), file.Scheme)
	if ext := path.Ext(name); ext != "" {
		name = name[:len(name)-len(ext)]
	}
	baseURL := g.GoCodeLocation()
	return url.Join(baseURL, prefix+methodFragment+"handler.go")
}

func (g *Generate) HandlerType(prefix string, methodFragment string) string {
	result := prefix + methodFragment + "Handler"
	if g.Package() == "" {
		return result
	}
	return g.Package() + "." + result
}

func (g *Generate) InputType(prefix string, methodFragment string) string {
	result := prefix + methodFragment + "Input"
	if g.Package() == "" {
		return result
	}
	return g.Package() + "." + result
}

func (g *Generate) IndexLocation(prefix string, methodFragment string) string {
	_, name := url.Split(g.SourceURL(), file.Scheme)
	if ext := path.Ext(name); ext != "" {
		name = name[:len(name)-len(ext)]
	}
	baseURL := g.GoCodeLocation()
	return url.Join(baseURL, prefix+methodFragment+"index.go")
}

func (g *Generate) InitLocation(prefix string, methodFragment string) string {
	_, name := url.Split(g.SourceURL(), file.Scheme)
	if ext := path.Ext(name); ext != "" {
		name = name[:len(name)-len(ext)]
	}
	baseURL := g.GoCodeLocation()
	return url.Join(baseURL, prefix+methodFragment+"init.go")
}

func (g *Generate) OutputType(prefix string, methodFragment string) string {
	result := prefix + methodFragment + "Output"
	if g.Package() == "" {
		return result
	}
	return g.Package() + "." + result
}
