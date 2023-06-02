package options

import "github.com/viant/afs/url"

type Touch struct {
	Repo      string `short:"r" long:"repo" description:"datly rule repository location" `
	RoutesURL string
}

func (t *Touch) Init() {
	t.Repo = ensureAbsPath(t.Repo)
	t.RoutesURL = url.Join(t.Repo, "Datly/routes")
}
