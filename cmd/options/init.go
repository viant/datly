package options

type Init struct {
	Project string `short:"p" long:"proj" description:"project location"`
	Repository
	CacheProvider
}

func (i *Init) Init() error {
	i.Project = ensureAbsPath(i.Project)
	i.Repository.Init(i.Project)
	if i.Port == nil {
		port := 8080
		i.Port = &port
	}
	return i.CacheProvider.Init()
}
