// Package build owns custom project initialization and compilation. The
// application-owned internal/datlylink package selects compiled components.
// The service is independent of CLI flags and can be reused by developer tools.
package build

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/viant/datly/bootstrap"
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
	Binary, SHA256               string
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
	selection, err := (xmodule.BuildWorkspace{BaseDir: root, Patterns: request.Packages, Tags: request.Tags, Env: request.Env}).Resolve(ctx)
	if err != nil {
		return nil, err
	}
	routes, err := (bootstrap.PackageDiscovery{
		Workspace: selection.Workspace(),
		Include:   []string{"..."},
		Exclude:   []string{info.Path + "/cmd/datly", info.Path + "/internal/datlylink"},
	}).Discover(ctx)
	if err != nil {
		return nil, err
	}
	result := &Result{Components: len(routes)}
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
	binary, err := os.ReadFile(temp.Name())
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(binary)
	result.SHA256 = hex.EncodeToString(digest[:])
	if err = os.Rename(temp.Name(), output); err != nil {
		return nil, err
	}
	result.Binary = output
	return result, nil
}
