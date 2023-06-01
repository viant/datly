package options

type Init struct {
	Project string `short:"p" long:"proj" description:"project location"`
	Repository
}

func (i *Init) Init() error {
	i.Project = ensureAbsPath(i.Project)
	i.Repository.Init(i.Project)
	return nil
}
