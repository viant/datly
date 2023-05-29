package options

import (
	"github.com/viant/afs/url"
	"os"
)

type Gen struct {
	Connector
	Generate
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
	if g.Dest != "" && url.IsRelative(g.Dest) {
		wd, _ := os.Getwd()
		g.Dest = url.Join(wd, g.Dest)
	}
	if g.Source != "" && url.IsRelative(g.Source) {
		wd, _ := os.Getwd()
		g.Source = url.Join(wd, g.Source)
	}
	return nil
}
