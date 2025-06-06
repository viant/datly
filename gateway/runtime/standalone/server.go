package standalone

import (
	"context"
	"fmt"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/gateway/runtime/standalone/handler"
	"github.com/viant/datly/mcp"
	"github.com/viant/datly/view/extension"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"time"
)

type Server struct {
	http.Server
	Service      *gateway.Service
	useSingleton *bool //true by default
	MCP          *mcp.Server
}

// shutdownOnInterrupt server on interupts
func (r *Server) shutdownOnInterrupt() {
	closed := make(chan struct{})
	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt)
		<-sigint
		// We received an interrupt signal, shut down.
		if err := r.Shutdown(context.Background()); err != nil {
			// Error from closing listeners, or context timeout:
			log.Printf("HTTP server Shutdown: %v", err)
		}
		close(closed)
	}()
}

func New(ctx context.Context, opts ...Option) (*Server, error) {
	service, config, err := NewService(ctx, opts...)
	if err != nil {
		if service != nil {
			_ = service.Close()
		}
		return nil, err
	}
	server := &Server{
		Service: service,
		Server: http.Server{
			Addr:           ":" + strconv.Itoa(config.Endpoint.Port),
			Handler:        service,
			ReadTimeout:    time.Millisecond * time.Duration(config.Endpoint.ReadTimeoutMs),
			WriteTimeout:   time.Millisecond * time.Duration(config.Endpoint.WriteTimeoutMs),
			MaxHeaderBytes: config.Endpoint.MaxHeaderBytes,
		},
	}
	if config.MCP != nil && config.MCP.Port != nil {
		server.MCP, err = mcp.NewServer(service.MCP(), config.MCP)
		if err != nil {
			return nil, err
		}
	}
	server.shutdownOnInterrupt()
	return server, nil
}

func NewService(ctx context.Context, opts ...Option) (*gateway.Service, *Config, error) {
	options, err := NewOptions(ctx, opts...)
	if err != nil {
		return nil, nil, err
	}
	config := options.config
	config.Init(ctx)
	//mux := http.NewServeMux()
	if config.Config == nil {
		return nil, nil, fmt.Errorf("gateway config was empty")
	}
	var service *gateway.Service
	var gOptions = []gateway.Option{
		gateway.WithExtensions(extension.Config),
		gateway.WithStatusHandler(handler.NewStatus(config.Version, &config.Meta)),
	}
	if options.options != nil {
		gOptions = append(gOptions, options.options...)
	}
	if options.UseSingleton() {
		service, err = gateway.Singleton(ctx, gOptions...)
	} else {
		service, err = gateway.New(ctx, gOptions...)
	}
	return service, config, err
}
