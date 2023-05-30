package command

import (
	"context"
	_ "embed"
	"github.com/viant/afs"
	"github.com/viant/datly/cmd/options"
	"os"
	"os/exec"
)

const (
	datlyFolder = "datly"
	pkgFolder   = "pkg"
	dsqlFolder  = "dsql"

	datlyHeadURL = "https:github.com/viant/datly/archive/refs/heads/master.zip/zip://localhost/"
	datlyTagURL  = "https:github.com/viant/datly/archive/refs/tags/%v.zip/zip://localhost/"
)

type Service struct {
	fs         afs.Service
	goLocation string
}

func (s *Service) Run(ctx context.Context, opts *options.Options) (bool, error) {
	if opts.InitExt != nil {
		return true, s.runInitExtension(ctx, opts.InitExt)
	}
	if opts.Bundle != nil {
		return true, s.bundleRules(ctx, opts.Bundle)
	}
	return false, nil
}

func (s *Service) runCommand(dir string, cmd string, args ...string) (string, error) {
	command := exec.Command(cmd, args...)
	command.Env = os.Environ()
	command.Dir = dir
	output, err := command.CombinedOutput()
	if err != nil {
		return "", err
	}
	return string(output), nil
}

func New() *Service {
	return &Service{
		fs: afs.New(),
	}
}
