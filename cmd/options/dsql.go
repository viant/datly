package options

type DSql struct {
	Generate
	Repository
	RoutePrefix string `short:"f" long:"routePrefix" description:"routePrefix default: dev/"`
}

func (d *DSql) Init() error {
	if err := d.Generate.Init(); err != nil {
		return err
	}
	return d.Repository.Init(d.Project)
}
