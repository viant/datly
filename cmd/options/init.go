package options

import "context"

type Init struct {
	Project string `short:"p" long:"proj" description:"project location"`
	Repository
	CacheProvider
}

func (i *Init) Init(ctx context.Context) error {
	i.Project = ensureAbsPath(i.Project)
	i.Repository.Init(ctx, i.Project)
	if i.Port == nil {
		port := 8080
		i.Port = &port
	}
	return i.CacheProvider.Init()
}
