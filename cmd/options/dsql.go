package options

import "github.com/viant/afs/url"

type DSql struct {
	Connector
	JwtVerifier
	Generate
	Dest        string `short:"d" long:"dest" description:"datly rule repository location"`
	Const       string `short:"C" long:"const" description:"const location" `
	Port        int    `short:"P" long:"port" description:"endpoint port" `
	RoutePrefix string `short:"f" long:"routePrefix" description:"routePrefix default: dev/"`
}

func (d *DSql) Init() error {
	if err := d.Generate.Init(); err != nil {
		return err
	}
	if url.IsRelative(d.Dest) && d.Project != "" {
		d.Dest = url.Join(d.Project, d.Dest)
	}
	return nil
}
