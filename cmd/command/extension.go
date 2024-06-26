package command

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/options"
	rdata "github.com/viant/toolbox/data"
	"os"
	"os/exec"
	"path"
	"strings"
)

const (
	goInitVersion = "v0.0.0-00010101000000-000000000000"
)

func (s *Service) RunInitExtension(ctx context.Context, init *options.Extension) (err error) {
	customDatlySrc := url.Join(init.Datly.Location, datlyFolder)
	hasDatlyBinary, _ := s.fs.Exists(ctx, customDatlySrc)
	if !hasDatlyBinary {
		if err = s.ensureDatly(ctx, init); err != nil {
			return err
		}
	}
	dSQLLoc := url.Join(init.Project, dqlFolder)
	if ok, _ := s.fs.Exists(ctx, dSQLLoc); !ok {
		if err = s.fs.Create(ctx, dSQLLoc, file.DefaultDirOsMode, true); err != nil {
			return err
		}
	}
	pkgDest := init.PackageLocation()
	hasPackage, _ := s.fs.Exists(ctx, pkgDest)
	if hasPackage {
		fmt.Printf("updating '%v' ...\n", pkgDest)
		if err = s.updatePackage(ctx, pkgDest, init); err != nil {
			return fmt.Errorf("failed to update package: %w", err)
		}

	} else {
		fmt.Printf("generating %v ...\n", pkgDest)
		if err = s.generatePackage(ctx, pkgDest, init); err != nil {
			return fmt.Errorf("failed to generate package: %w", err)
		}
	}

	fmt.Printf("generating ext %v ...\n", s.extLocation(init))
	if err = s.generateExtensionModule(ctx, init); err != nil {
		return err
	}
	datlyLocation := url.Join(init.Datly.Location, "datly")
	fmt.Printf("extending custom datly: %v ...\n", datlyLocation)
	if err = s.extendDatly(ctx, init); err != nil {
		return nil
	}
	return nil
}

//go:embed tmpl/ext/go.modx
var extGoModuleContent string

//go:embed tmpl/ext/init.gox
var extContent string

//go:embed tmpl/pkg/plugin/init.gox
var pluginContent string

func (s *Service) generateExtensionModule(ctx context.Context, init *options.Extension) error {
	extLocation := s.extLocation(init)
	replacer := init.Replacer(&init.Module)
	if err := s.fs.Upload(ctx, path.Join(extLocation, "init.go"), file.DefaultFileOsMode, strings.NewReader(replacer.ExpandAsText(extContent))); err != nil {
		return err
	}
	if err := s.fs.Upload(ctx, path.Join(extLocation, "go.mod"), file.DefaultFileOsMode, strings.NewReader(replacer.ExpandAsText(extGoModuleContent))); err != nil {
		return err
	}
	return s.syncSourceDependencies(ctx, extLocation)
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
	cmd := exec.Command("mv", moveSource, moveDest)
	if _, err := cmd.Output(); err != nil {
		return err
	}
	return nil
}

func (s *Service) extendDatly(ctx context.Context, init *options.Extension) error {
	goBinLocation, err := s.getGoBinLocation(ctx)
	if err != nil {
		return err
	}
	pkgLocation := url.Join(init.Project, "pkg")
	datlyLocation := url.Join(init.Datly.Location, "datly")
	extLocation := s.extLocation(init)
	_, err = s.runCommand(datlyLocation, goBinLocation, "mod", "edit", "-require", init.Module.Module()+"@"+goInitVersion)
	_, err = s.runCommand(datlyLocation, goBinLocation, "mod", "edit", "-replace", init.Module.Module()+"="+pkgLocation)
	_, err = s.runCommand(datlyLocation, goBinLocation, "mod", "edit", "-replace", "github.com/viant/xdatly/extension="+extLocation)
	if err = s.syncSourceDependencies(ctx, datlyLocation); err != nil {
		return err
	}
	return err
}

func (s *Service) extLocation(init *options.Extension) string {
	extLoc := url.Join(init.Project, extFolder)
	return extLoc
}

//go:embed tmpl/pkg/bootstrap/bootstrap.gox
var bootstrapContent string

//go:embed tmpl/pkg/checksum/init.gox
var checksumContent string

//go:embed tmpl/pkg/dependency/init.gox
var dependecnyContent string

//go:embed tmpl/pkg/init.gox
var initContent string

func (s *Service) updatePackage(ctx context.Context, pkgLocation string, init *options.Extension) (err error) {
	replacer := init.Replacer(&init.Module)
	err = s.ensureDependencies(ctx, pkgLocation, replacer)
	if err != nil {
		return err
	}

	if err = s.ensureChecksum(ctx, pkgLocation, replacer); err != nil {
		return fmt.Errorf("failed to ensure checksum %s", err)
	}
	if err = s.ensureDefaultImportInit(ctx, pkgLocation, replacer); err != nil {
		return fmt.Errorf("failed to ensure default imports %s", err)
	}
	if err = s.ensurePluginEntrypoint(ctx, pkgLocation, replacer); err != nil {
		return fmt.Errorf("failed to ensure plugin entrypoint %s", err)
	}
	if err = s.syncSourceDependencies(ctx, pkgLocation); err != nil {
		return fmt.Errorf("failed to sync source dependencies %s", err)
	}
	return nil
}

func (s *Service) generatePackage(ctx context.Context, pkgLocation string, init *options.Extension) error {
	goBinLocation, err := s.getGoBinLocation(ctx)
	if err != nil {
		return err
	}
	if err = s.updatePackage(ctx, pkgLocation, init); err != nil {
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

func (s *Service) ensureDependencies(ctx context.Context, pkgLocation string, replacer rdata.Map) error {
	dependencyDest := url.Join(pkgLocation, "dependency/init.go")
	if ok, _ := s.fs.Exists(ctx, dependencyDest); ok {
		return nil
	}
	boostrapDest := url.Join(pkgLocation, "bootstrap/bootstrap.go")
	if err := s.fs.Upload(ctx, boostrapDest, file.DefaultFileOsMode, strings.NewReader(replacer.ExpandAsText(bootstrapContent))); err != nil {
		return err
	}
	if err := s.fs.Upload(ctx, dependencyDest, file.DefaultFileOsMode, strings.NewReader(replacer.ExpandAsText(dependecnyContent))); err != nil {
		return err
	}
	return nil
}

func (s *Service) ensurePluginEntrypoint(ctx context.Context, pkgLocation string, replacer rdata.Map) error {
	pluginDst := url.Join(pkgLocation, "plugin/main.go")
	if ok, _ := s.fs.Exists(ctx, pluginDst); ok {
		return nil
	}
	if err := s.fs.Upload(ctx, pluginDst, file.DefaultFileOsMode, strings.NewReader(replacer.ExpandAsText(pluginContent))); err != nil {
		return err
	}
	return nil
}

func (s *Service) ensureDefaultImportInit(ctx context.Context, pkgLocation string, replacer rdata.Map) error {
	initDest := url.Join(pkgLocation, "init.go")
	if ok, _ := s.fs.Exists(ctx, initDest); ok {
		return nil
	}
	if err := s.fs.Upload(ctx, initDest, file.DefaultFileOsMode, strings.NewReader(replacer.ExpandAsText(initContent))); err != nil {
		return err
	}
	return nil
}

func (s *Service) ensureChecksum(ctx context.Context, pkgLocation string, replacer rdata.Map) error {
	checksumDest := url.Join(pkgLocation, "dependency/checksum/init.go")
	if ok, _ := s.fs.Exists(ctx, checksumDest); ok {
		return nil
	}
	if err := s.fs.Upload(ctx, checksumDest, file.DefaultFileOsMode, strings.NewReader(replacer.ExpandAsText(checksumContent))); err != nil {
		return err
	}
	return nil
}

func (s *Service) syncSourceDependencies(ctx context.Context, pkgLocation string) error {
	goBinLocation, err := s.getGoBinLocation(ctx)
	if err != nil {
		return err
	}
	_, err = s.runCommand(pkgLocation, goBinLocation, "mod", "tidy")
	if err != nil {
		return fmt.Errorf("failed to run go mod tidy: %w ", err)
	}
	return nil
}

func (s *Service) getGoBinLocation(ctx context.Context) (string, error) {
	if s.goBinLocation != "" {
		return s.goBinLocation, nil
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
