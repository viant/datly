package developer

import (
	"context"
	"fmt"
	"io"
	"net"
	"slices"
	"strconv"

	"github.com/google/uuid"
	"github.com/viant/datly/standalone"
)

type Instance struct {
	ID      string `json:"instanceId"`
	Target  string `json:"target"`
	Address string `json:"address"`
	Status  string `json:"status"`
}
type instance struct {
	state  Instance
	server *standalone.Server
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

func (s *Service) run(ctx context.Context, args arguments) (*Instance, error) {
	app, ok := s.applications[args.Target]
	if !ok {
		return nil, fmt.Errorf("run is not configured for this target")
	}
	port := app.Ports[0]
	if args.Port != nil {
		port = *args.Port
	}
	address := args.Address
	if address == "" {
		address = app.Addresses[0]
	}
	if !slices.Contains(app.Ports, port) || !slices.Contains(app.Addresses, address) {
		return nil, fmt.Errorf("requested bind address or port is not configured")
	}
	s.mu.Lock()
	if len(s.instances) >= s.limit {
		s.mu.Unlock()
		return nil, fmt.Errorf("developer instance limit reached")
	}
	for _, running := range s.instances {
		if running.state.Target == args.Target {
			s.mu.Unlock()
			return nil, fmt.Errorf("target already has an owned instance")
		}
	}
	s.mu.Unlock()
	options := app.Options
	cfg := *options.Config
	cfg.Endpoint.Address = net.JoinHostPort(address, strconv.Itoa(port))
	cfg.Endpoint.Port = 0
	options.Config = &cfg
	server, err := standalone.New(ctx, options)
	if err != nil {
		return nil, err
	}
	lifetime, cancel := context.WithCancel(s.ctx)
	i := &instance{state: Instance{ID: uuid.NewString(), Target: args.Target, Status: "starting"}, server: server, cancel: cancel, done: make(chan struct{})}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		cancel()
		_ = server.Shutdown(context.Background())
		return nil, fmt.Errorf("developer service is closed")
	}
	s.instances[i.state.ID] = i
	s.work.Add(1)
	s.mu.Unlock()
	go func() {
		err := server.Serve(lifetime, io.Discard)
		// Join cleanup even if Serve returned its shutdown waiting deadline.
		_ = server.Shutdown(context.Background())
		s.mu.Lock()
		i.err = err
		i.state.Status = "stopped"
		if err != nil {
			i.state.Status = "failed"
		}
		delete(s.instances, i.state.ID)
		s.history[i.state.ID] = i.state
		s.order = append(s.order, i.state.ID)
		if len(s.order) > 64 {
			delete(s.history, s.order[0])
			s.order = s.order[1:]
		}
		close(i.done)
		s.mu.Unlock()
		cancel()
		s.work.Done()
	}()
	stopStartup := context.AfterFunc(ctx, cancel)
	addresses, err := server.WaitReady(ctx)
	stopStartup()
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		cancel()
		select {
		case <-i.done:
			s.mu.Lock()
			startupErr := i.err
			s.mu.Unlock()
			if startupErr != nil {
				return nil, startupErr
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-i.done:
		return nil, fmt.Errorf("standalone stopped during startup: %v", i.err)
	default:
	}
	i.state.Address = "http://" + addresses[0]
	i.state.Status = "running"
	result := i.state
	return &result, nil
}

func (s *Service) stop(ctx context.Context, id string) (*Instance, error) {
	s.mu.Lock()
	if completed, ok := s.history[id]; ok {
		s.mu.Unlock()
		return &completed, nil
	}
	i, ok := s.instances[id]
	if !ok {
		s.mu.Unlock()
		return nil, fmt.Errorf("unknown owned instance")
	}
	i.state.Status = "stopping"
	i.cancel()
	s.mu.Unlock()
	select {
	case <-i.done:
		s.mu.Lock()
		result := i.state
		err := i.err
		s.mu.Unlock()
		return &result, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Shutdown cancels and joins owned instances and authoring operations. A caller
// deadline bounds waiting; subsequent calls join the same cleanup.
func (s *Service) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("developer shutdown context is required")
	}
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.cancel()
		go func() { s.work.Wait(); close(s.done) }()
	}
	s.mu.Unlock()
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
