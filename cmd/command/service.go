package command

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/options"
	"os"
	"os/exec"
	"path"
	"strings"
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
		return true, s.runInit(ctx, opts.InitExt)
	}
	return false, nil
}

func (s *Service) runInit(ctx context.Context, init *options.Extension) error {

	if ok, _ := s.fs.Exists(ctx, url.Join(init.Datly.Location, datlyFolder)); !ok {
		if err := s.ensureDatly(ctx, init); err != nil {
			return err
		}
	}
	dSQLLoc := url.Join(init.Project, dsqlFolder)
	if ok, _ := s.fs.Exists(ctx, dSQLLoc); !ok {
		if err := s.fs.Create(ctx, dSQLLoc, file.DefaultDirOsMode, true); err != nil {
			return err
		}
	}
	pkgDest := url.Join(init.Project, pkgFolder)
	if ok, _ := s.fs.Exists(ctx, pkgDest); !ok {
		if err := s.generatePackage(ctx, pkgDest, init); err != nil {
			return err
		}
	}
	if err := s.extendDatly(ctx, init); err != nil {
		return nil
	}
	return nil
}

func (s *Service) ensureDatly(ctx context.Context, init *options.Extension) error {
	_ = s.fs.Create(ctx, init.Datly.Location, file.DefaultDirOsMode, true)
	sourceURL := datlyHeadURL
	moveSource := path.Join(init.Datly.Location, datlyFolder+"-master")
	moveDest := path.Join(init.Datly.Location, datlyFolder)
	if init.Tag != "" {
		sourceURL = fmt.Sprintf(datlyTagURL, init.Tag)
		moveSource = path.Join(init.Datly.Location, datlyFolder+"-"+init.Tag[1:])
	}
	if err := s.fs.Copy(ctx, sourceURL, init.Datly.Location); err != nil {
		return err
	}
	if err := s.fs.Move(ctx, moveSource, moveDest); err != nil {
		return err
	}
	//s.fs.Delete(ctx)
	return nil
}

func (s *Service) extendDatly(ctx context.Context, init *options.Extension) error {
	goBinLocation, err := s.getGoBinLocation(ctx)
	if err != nil {
		return err
	}
	datlyLocation := url.Join(init.Datly.Location, "datly")
	pkgDest := url.Join(init.Project, pkgFolder)
	_, err = s.runCommand(datlyLocation, goBinLocation, "mod", "edit", "-replace", "github.com/viant/xdatly/extension="+pkgDest)
	if err = s.syncSourceDependencies(ctx, datlyLocation); err != nil {
		return err
	}
	return err
}

//go:embed tmpl/pkg/bootstrap/bootstrap.gox
var bootstrapContent string

//go:embed tmpl/pkg/checksum/init.gox
var checksumContent string

func (s *Service) generatePackage(ctx context.Context, pkgLocation string, init *options.Extension) error {
	goBinLocation, err := s.getGoBinLocation(ctx)
	if err != nil {
		return err
	}
	replacer := init.Replacer(&init.Module)
	boostrapDest := url.Join(pkgLocation, "bootstrap/bootstrap.go")
	if err := s.fs.Upload(ctx, boostrapDest, file.DefaultFileOsMode, strings.NewReader(replacer.ExpandAsText(bootstrapContent))); err != nil {
		return err
	}
	checksumDest := url.Join(pkgLocation, "checksum/init.go")
	if err := s.fs.Upload(ctx, checksumDest, file.DefaultFileOsMode, strings.NewReader(replacer.ExpandAsText(checksumContent))); err != nil {
		return err
	}
	if _, err = s.runCommand(pkgLocation, goBinLocation, init.GoModInitArgs(&init.Module)...); err != nil {
		return err
	}
	if err := s.syncSourceDependencies(ctx, pkgLocation); err != nil {
		return err
	}
	return nil
}

func (s *Service) syncSourceDependencies(ctx context.Context, pkgLocation string) error {
	goBinLocation, err := s.getGoBinLocation(ctx)
	if err != nil {
		return err
	}
	_, err = s.runCommand(pkgLocation, goBinLocation, "mod", "tidy", "-compat=1.17")
	if err != nil {
		return err
	}
	return nil
}

func (s *Service) getGoBinLocation(ctx context.Context) (string, error) {
	if s.goLocation != "" {
		return s.goLocation, nil
	}
	goBinLocation, err := s.locateBinary(ctx, "go", "/usr/local/go/bin")
	if err != nil {
		return "", err
	}
	return goBinLocation, err
}

func (s *Service) locateBinary(ctx context.Context, app string, defaultPath string) (string, error) {
	knownPaths := os.Getenv("PATH")
	candidatePaths := append([]string{defaultPath}, strings.Split(knownPaths, ":")...)
	for _, loc := range candidatePaths {
		canidate := path.Join(loc, app)
		if ok, _ := s.fs.Exists(ctx, canidate); ok {
			return canidate, nil
		}
	}
	return "", fmt.Errorf("failed to locate: %v", app)
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
