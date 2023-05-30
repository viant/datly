package options

import (
	"github.com/viant/afs/url"
	"path"
)

type Gen struct {
	Connector
	Generate
	Dest      string `short:"d" long:"dest" description:"dsql location"`
	Operation string `short:"o" long:"op" description:"operation" choice:"post" choice:"patch" choice:"put"`
	Kind      string `short:"k" long:"kind" description:"execution kind" choice:"dml" choice:"sql"`
	LoadPrev  bool   `short:"l" long:"loadPrev" description:"load previous state" `
}

func (g *Gen) Init() error {
	if err := g.Generate.Init(); err != nil {
		return err
	}
	if g.Dest == "" {
		g.Dest = "dsql"
	}
	if url.IsRelative(g.Source) && g.Project != "" {
		g.Source = path.Join(g.Project, g.Source)
	}
	if url.IsRelative(g.Dest) && g.Project != "" {
		g.Dest = path.Join(g.Project, g.Dest)
	}

	g.Source = ensureAbsPath(g.Source)
	g.Dest = ensureAbsPath(g.Dest)
	return nil
}
