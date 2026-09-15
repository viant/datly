package developer

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/viant/datly/transcribe"
)

func (s *Service) configure(config Config) error {
	s.authoring = map[string]transcribe.Request{}
	for name, request := range config.Authoring {
		target, ok := s.targets[name]
		if !ok || request.Source == nil {
			return fmt.Errorf("authoring requires a configured target and source")
		}
		base, err := filepath.EvalSymlinks(target.BaseDir)
		if err != nil {
			return err
		}
		// Report generated files against the same root used for containment.
		target.BaseDir = base
		s.targets[name] = target
		destination, err := filepath.Abs(request.Destination)
		if err != nil || request.Destination == "" {
			return fmt.Errorf("authoring destination is required")
		}
		destination, err = filepath.EvalSymlinks(destination)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(base, destination)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
			return fmt.Errorf("authoring destination escapes configured target")
		}
		request.Destination = destination
		source := *request.Source
		request.Source = &source
		if source.Types != nil {
			request.Source.Types, err = source.Types.Clone()
			if err != nil {
				return err
			}
		}
		request, err = request.NormalizeGeneration()
		if err != nil {
			return fmt.Errorf("authoring target %q: %w", name, err)
		}
		s.authoring[name] = request
	}
	s.applications = map[string]Application{}
	for name, app := range config.Applications {
		if _, ok := s.targets[name]; !ok || app.Options.Config == nil {
			return fmt.Errorf("application requires a configured target and standalone config")
		}
		app.Ports = append([]int(nil), app.Ports...)
		app.Addresses = append([]string(nil), app.Addresses...)
		if len(app.Ports) == 0 {
			app.Ports = []int{0}
		}
		if len(app.Addresses) == 0 {
			app.Addresses = []string{"127.0.0.1"}
		}
		for _, port := range app.Ports {
			if port < 0 || port > 65535 {
				return fmt.Errorf("invalid application port")
			}
		}
		for _, address := range app.Addresses {
			if ip := net.ParseIP(address); ip == nil || !ip.IsLoopback() {
				return fmt.Errorf("developer bind addresses must be literal loopback IPs")
			}
		}
		s.applications[name] = app
	}
	s.limit = config.MaxInstances
	if s.limit == 0 {
		s.limit = 16
	}
	if s.limit < 1 || s.limit > 256 {
		return fmt.Errorf("invalid developer instance limit")
	}
	s.workspace = make(chan struct{}, 1)
	s.instances = map[string]*instance{}
	s.history = map[string]Instance{}
	s.done = make(chan struct{})
	s.ctx, s.cancel = context.WithCancel(context.Background())
	return nil
}

func (s *Service) operation(ctx context.Context) (context.Context, func(), error) {
	if ctx == nil {
		return nil, nil, fmt.Errorf("developer operation context is required")
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, nil, fmt.Errorf("developer service is closed")
	}
	s.work.Add(1)
	s.mu.Unlock()
	operation, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	finish := func() { stop(); cancel(); s.work.Done() }
	select {
	case s.workspace <- struct{}{}:
		return operation, func() { <-s.workspace; finish() }, nil
	case <-operation.Done():
		finish()
		return nil, nil, operation.Err()
	}
}
