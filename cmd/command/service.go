package command

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/internal/asset"
	"github.com/viant/datly/internal/codegen"
	"github.com/viant/datly/internal/codegen/ast"
	"github.com/viant/datly/internal/plugin"
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

type (
	Service struct {
		fs            afs.Service
		goBinLocation string
	}
)

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
	if err := s.tidyModule(ctx, build.Module); err != nil {
		return err
	}
	if err := s.tidyModule(ctx, build.Datly); err != nil {
		return err
	}
	return nil
}

func (s *Service) tidyModule(ctx context.Context, goModule string) error {
	goBinLoc, err := s.getGoBinLocation(ctx)
	if err != nil {
		return fmt.Errorf("failed to preapre build, unable to find go %w", err)
	}
	if out, err := s.runCommand(goModule, goBinLoc, "mod", "tidy", "-compat=1.17"); err != nil {
		return fmt.Errorf("failed to go mod module '%v', %s %w", goModule, out, err)
	}
	return nil
}

func (s *Service) generateTemplateFiles(gen *options.Generate, template *codegen.Template, info *plugin.Info, opts ...codegen.Option) ([]*asset.File, error) {

	var files []*asset.File
	switch gen.Lang {
	case ast.LangGO:

		handler, index, err := template.GenerateHandler(gen, info)
		if err != nil {
			return nil, err
		}
		files = append(files, asset.NewFile(gen.HandlerLocation(), handler))
		files = append(files, asset.NewFile(gen.IndexLocation(), index))
		fallthrough
	case ast.LangVelty:
		dSQLContent, err := template.GenerateDSQL(opts...)
		if err != nil {
			return nil, err
		}
		files = append(files, asset.NewFile(gen.DSQLLocation(), dSQLContent))

	default:
		return nil, fmt.Errorf("unsupported lang type %v", gen.Lang)
	}
	return files, nil
}

func New() *Service {
	return &Service{
		fs: afs.New(),
	}
}
