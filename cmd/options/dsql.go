package options

import "context"

type Translate struct {
	Rule
	Repository
	RoutePrefix string `short:"f" long:"routePrefix" description:"routePrefix default: dev/"`
}

func (d *Translate) Init(ctx context.Context) error {
	if err := d.Rule.Init(); err != nil {
		return err
	}
	return d.Repository.Init(ctx, d.Project)
}
