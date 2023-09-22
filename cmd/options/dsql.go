package options

import "context"

type Translate struct {
	Rule
	Repository
}

func (d *Translate) Init(ctx context.Context) error {
	if err := d.Rule.Init(); err != nil {
		return err
	}
	return d.Repository.Init(ctx, d.Project)
}
