package command

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/datly/cmd/options"
	"os"
	"os/exec"
)

const (
	datlyFolder = "datly"
	pkgFolder   = "pkg"
	extFolder   = ".build/ext"

	dsqlFolder = "dsql"

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
	if opts.Touch != nil {
		s.Touch(ctx, opts.Touch)
		return true, nil
	}
	if opts.Build != nil {
		return false, s.prepareBuild(ctx, opts.Build)
	}
	return false, nil
}

func (s *Service) runCommand(dir string, cmd string, args ...string) (string, error) {
	command := exec.Command(cmd, args...)
	command.Env = os.Environ()
	command.Dir = dir
	output, err := command.CombinedOutput()
	if err != nil {
		return string(output), err
	}
	return string(output), nil
}

func (s *Service) prepareBuild(ctx context.Context, build *options.Build) error {
	goBinLoc, err := s.getGoBinLocation(ctx)
	if err != nil {
		return fmt.Errorf("failed to preapre build, unable to find go %w", err)
	}
	if out, err := s.runCommand(build.Module, goBinLoc, "mod", "tidy"); err != nil {
		return fmt.Errorf("failed to go mod module '%v', %s %w", build.Module, out, err)
	}
	if out, err := s.runCommand(build.Datly, goBinLoc, "mod", "tidy", "-compat=1.17"); err != nil {
		return fmt.Errorf("failed to go mod customized datly %v, %s %w", build.Module, out, err)
	}
	return nil
}

func New() *Service {
	return &Service{
		fs: afs.New(),
	}
}
