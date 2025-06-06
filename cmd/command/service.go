package command

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/internal/asset"
	"github.com/viant/datly/internal/codegen"
	"github.com/viant/datly/internal/codegen/ast"
	"github.com/viant/datly/internal/plugin"
	"github.com/viant/datly/internal/translator"
	"github.com/viant/pgo"

	"os"
	"os/exec"
)

const (
	datlyFolder = "datly"
	extFolder   = ".build/ext"

	dqlFolder = "dql"

	datlyHeadURL = "https:github.com/viant/datly/archive/refs/heads/master.zip/zip://localhost/"
	datlyTagURL  = "https:github.com/viant/datly/archive/refs/tags/%v.zip/zip://localhost/"
)

type (
	Service struct {
		fs            afs.Service
		config        *standalone.Config
		translator    *translator.Service
		goBinLocation string
		Files         asset.Files
	}
)

func (s *Service) Exec(ctx context.Context, opts *options.Options) error {

	if opts.InitExt != nil {
		return s.RunInitExtension(ctx, opts.InitExt)
	}
	if opts.Bundle != nil {
		return s.BundleRules(ctx, opts.Bundle)
	}
	if opts.Touch != nil {
		s.Touch(ctx, opts.Touch)
		return nil
	}
	if opts.Build != nil {
		return s.PrepareBuild(ctx, opts.Build)
	}
	if opts.Plugin != nil {
		return s.BuildPlugin(ctx, opts.Plugin)
	}
	if opts.Generate != nil {
		return s.Generate(ctx, opts)
	}
	if opts.Translate != nil {
		return s.Translate(ctx, opts)
	}

	if opts.Mcp != nil {
		return s.Mcp(ctx, opts)
	}
	if opts.Run != nil {
		return s.Run(ctx, opts)
	}
	if opts.Cache != nil {
		return s.WarmupCache(ctx, opts.Cache)
	}

	return nil
}

func (s *Service) runCommand(dir string, cmd string, args ...string) (string, error) {
	command := exec.Command(cmd, args...)
	command.Env = os.Environ()
	command.Dir = dir
	output, err := command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to run command:cd %v &&  %v , %w, %s", dir, command.Args, err, string(output))
	}
	return string(output), nil
}

func (s *Service) PrepareBuild(ctx context.Context, aBuild *options.Build) error {
	if err := s.tidyModule(ctx, aBuild.Module); err != nil {
		return err
	}
	if err := s.tidyModule(ctx, aBuild.Datly); err != nil {
		return err
	}

	flags := ""
	if aBuild.LdFlags != nil {
		flags = *aBuild.LdFlags
	}
	return pgo.Build(&pgo.Options{
		Name:        aBuild.Name,
		SourceURL:   aBuild.Source,
		DestURL:     aBuild.DestURL,
		Arch:        aBuild.GoArch,
		Os:          aBuild.GoOs,
		Version:     aBuild.GoVersion,
		MainPath:    aBuild.MainPath,
		BuildArgs:   aBuild.BuildArgs,
		GoRoot:      aBuild.GoRoot,
		GoPath:      aBuild.GoPath,
		BuildMode:   "exec",
		Compression: "gzip",
		WithLogger:  true,
		LdFlags:     flags,
	})
}

func (s *Service) tidyModule(ctx context.Context, goModule string) error {
	goBinLoc, err := s.getGoBinLocation(ctx)
	if err != nil {
		return fmt.Errorf("failed to preapre build, unable to find go %w", err)
	}
	if out, err := s.runCommand(goModule, goBinLoc, "mod", "tidy"); err != nil {
		return fmt.Errorf("failed to go mod module '%v', %s %w", goModule, out, err)
	}
	return nil
}

func (s *Service) generateTemplateFiles(gen *options.Generate, template *codegen.Template, info *plugin.Info, opts ...codegen.Option) error {
	switch gen.Lang {
	case ast.LangGO:
		handler, index, init, err := template.GenerateHandler(gen, info)
		if err != nil {
			return err
		}
		s.Files.Append(asset.NewFile(gen.HandlerLocation(template.FilePrefix(), template.FileMethodFragment()), handler))
		s.Files.Append(asset.NewFile(gen.IndexLocation(template.FilePrefix(), template.FileMethodFragment()), index))
		s.Files.Append(asset.NewFile(gen.InitLocation(template.FilePrefix(), template.FileMethodFragment()), init))

		fallthrough
	case ast.LangVelty:
		dSQLContent, err := template.GenerateDSQL(opts...)
		if err != nil {
			return err
		}
		gen.Rule.Output = append(gen.Rule.Output, gen.DSQLLocation())
		s.Files.Append(asset.NewFile(gen.DSQLLocation(), dSQLContent))
	default:
		return fmt.Errorf("unsupported lang type %v", gen.Lang)
	}
	return nil
}

func New() *Service {
	return &Service{
		fs: afs.New(),
	}
}
