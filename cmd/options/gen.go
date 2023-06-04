package options

import (
	"fmt"
	"github.com/viant/afs/url"
)

type Gen struct {
	Connector
	Generate
	Package   string `short:"g" long:"pkg" description:"entity package"`
	Dest      string `short:"d" long:"dest" description:"dsql location" default:"dsql"`
	Operation string `short:"o" long:"op" description:"operation" choice:"post" choice:"patch" choice:"put"`
	Kind      string `short:"k" long:"kind" description:"execution kind" choice:"dml" choice:"service"`
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
