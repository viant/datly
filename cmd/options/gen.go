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
	Dest           string `short:"d" long:"dest" description:"dql file location" default:"dql"`
	Operation      string `short:"o" long:"op" description:"operation" choice:"post" choice:"patch" choice:"put" choice:"get"`
	Kind           string `short:"k" long:"kind" description:"execution kind" choice:"dml" choice:"service"`
	Lang           string `short:"l" long:"lang" description:"lang" choice:"velty" choice:"go"`
	Translate      bool   `short:"t" long:"translate" description:"translate generated DSQL"`
	NoComponentDef bool   `short:"Z" long:"noComDef" description:"do not include component definition" `
}

func (g *Generate) HttpMethod() string {
	return strings.ToUpper(g.Operation)
}

func (g *Generate) EmbedLocation(URI string, methodFragment string) string {
	codeLocation := g.GoCodeLocation()
	return url.Join(codeLocation, methodFragment, URI)
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

func (g *Generate) EntityLocation(prefix, methodFragment, entityName string) string {
	return g.normalizeCodeLocation(prefix, methodFragment, entityName+".go")
}

func (g *Generate) InputLocation(prefix string, methodFragment string) string {
	return g.normalizeCodeLocation(prefix, methodFragment, "input.go")
}

func (g *Generate) InputInitLocation(prefix string, methodFragment string) string {
	return g.normalizeCodeLocation(prefix, methodFragment, "input_init.go")
}

func (g *Generate) InputValidateLocation(prefix string, methodFragment string) string {
	return g.normalizeCodeLocation(prefix, methodFragment, "input_validate.go")
}

func (g *Generate) OutputLocation(prefix string, methodFragment string) string {
	return g.normalizeCodeLocation(prefix, methodFragment, "output.go")
}

func (g *Generate) HandlerLocation(prefix string, methodFragment string) string {
	return g.normalizeCodeLocation(prefix, methodFragment, "handler.go")
}

func (g *Generate) IndexLocation(prefix string, methodFragment string) string {
	return g.normalizeCodeLocation(prefix, methodFragment, "index.go")
}

func (g *Generate) InitLocation(prefix string, methodFragment string) string {
	return g.normalizeCodeLocation(prefix, methodFragment, "init.go")
}

func (g *Generate) HandlerType(methodFragment string) string {
	return g.customType("Handler", methodFragment)
}

func (g *Generate) InputType(methodFragment string) string {
	return g.customType("Input", methodFragment)
}

func (g *Generate) OutputType(methodFragment string) string {
	return g.customType("Output", methodFragment)
}

func (g *Generate) customType(result string, methodFragment string) string {
	if g.Package() == "" {
		return result
	}
	pkg := g.Package()
	if g.ModulePrefix != "" {
		if strings.Contains(g.ModulePrefix, pkg) {
			pkg = g.ModulePrefix
		} else {
			pkg = url.Join(g.ModulePrefix, pkg)
		}
	}
	if index := strings.LastIndex(pkg, "/"); index != -1 {
		pkg = pkg[index+1:]
	}
	return pkg + "/" + strings.ToLower(methodFragment) + "." + result
}

func (g *Generate) normalizeCodeLocation(prefix string, methodFragment string, filename string) string {
	_, name := url.Split(g.SourceURL(), file.Scheme)
	if ext := path.Ext(name); ext != "" {
		name = name[:len(name)-len(ext)]
	}
	baseURL := g.GoCodeLocation()
	if strings.HasSuffix(baseURL, prefix) {
		prefix = ""
	}
	return url.Join(baseURL, prefix, methodFragment, filename)
}
