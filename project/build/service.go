// Package build owns custom project initialization and automatic compiled linking.
// The service is independent of CLI flags and can be reused by developer tools.
package build

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	xmodule "github.com/viant/x/module"
)

type Service struct{}
type Request struct {
	Dir, Output, Tags string
	// Env is the complete Go environment; nil inherits it, including GOWORK.
	Env []string
	// Packages uses Go package patterns. The default is ./... in the project module.
	Packages []string
}
type Result struct {
	Binary, LinkFile, SHA256     string
	Components, Types, Factories int
}

func (Service) Build(ctx context.Context, request Request) (_ *Result, err error) {
	root, err := filepath.Abs(request.Dir)
	if err != nil {
		return nil, err
	}
	info, err := xmodule.LocateLocal(root)
	if err != nil {
		return nil, err
	}
	if info.Dir != root {
		return nil, fmt.Errorf("build directory must be the module root: %s", info.Dir)
	}
	managed := managedFiles{root: root}
	unlock, err := managed.lock()
	if err != nil {
		return nil, err
	}
	defer unlock()
	previous, err := managed.read()
	if err != nil {
		return nil, err
	}
	overlay, cleanup, err := managed.overlay()
	if err != nil {
		return nil, err
	}
	defer cleanup()
	selection, err := (xmodule.BuildWorkspace{BaseDir: root, Patterns: request.Packages, Tags: request.Tags, Env: request.Env, Overlay: overlay}).Resolve(ctx)
	if err != nil {
		return nil, err
	}
	links := newLinker(selection)
	source, result, err := links.generate(ctx, info.Path)
	if err != nil {
		return nil, err
	}
	if err = managed.write(source); err != nil {
		return nil, err
	}
	succeeded := false
	defer func() {
		if !succeeded {
			if restoreErr := managed.restore(previous); restoreErr != nil {
				err = errors.Join(err, fmt.Errorf("restore previous linker: %w", restoreErr))
			}
		}
	}()
	output := request.Output
	if output == "" {
		output = filepath.Join("bin", "datly")
	}
	if !filepath.IsAbs(output) {
		output = filepath.Join(root, output)
	}
	if err = os.MkdirAll(filepath.Dir(output), 0755); err != nil {
		return nil, err
	}
	temp, err := os.CreateTemp(filepath.Dir(output), ".datly-build-*")
	if err != nil {
		return nil, err
	}
	temp.Close()
	defer os.Remove(temp.Name())
	args := []string{"build", "-o", temp.Name()}
	if request.Tags != "" {
		args = append(args, "-tags", request.Tags)
	}
	args = append(args, "./cmd/datly")
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = root
	cmd.Env = request.Env
	if data, buildErr := cmd.CombinedOutput(); buildErr != nil {
		return nil, fmt.Errorf("compile linked project (check exported contracts and factory signatures): %w\n%s", buildErr, data)
	}
	result.SHA256, err = managed.hashFile(temp.Name())
	if err != nil {
		return nil, err
	}
	if err = managed.commit(source); err != nil {
		return nil, err
	}
	if err = os.Rename(temp.Name(), output); err != nil {
		return nil, err
	}
	succeeded = true
	result.Binary = output
	result.LinkFile = filepath.Join(root, linkPath)
	return result, nil
}
