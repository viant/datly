package options

import (
	"context"
	"fmt"
	"os"

	"github.com/viant/afs/url"
)

// Validate defines options for shape-only DQL validation.
type Validate struct {
	Connector
	Source  []string `short:"s" long:"src" description:"DQL source file(s)"`
	Project string   `short:"p" long:"proj" description:"project location"`
	Strict  bool     `long:"strict" description:"enable strict compile mode"`
}

func (v *Validate) Init(ctx context.Context) error {
	_ = ctx
	if v.Project == "" {
		v.Project, _ = os.Getwd()
	}
	v.Project = ensureAbsPath(v.Project)
	v.Connector.Init()
	if len(v.Source) == 0 {
		return fmt.Errorf("validate: at least one --src is required")
	}
	for i := range v.Source {
		if url.IsRelative(v.Source[i]) {
			expandRelativeIfNeeded(&v.Source[i], v.Project)
		}
	}
	return nil
}
