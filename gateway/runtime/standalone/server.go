package standalone

import (
	"context"
	"fmt"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/gateway/registry"
	"github.com/viant/datly/gateway/runtime/standalone/handler"
	"github.com/viant/gmetric"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"time"
)

type Server struct {
	http.Server
}

//shutdownOnInterrupt server on interupts
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

func NewWithAuth(config *Config, auth AuthHandler) (*Server, error) {
	if auth == nil {
		auth = &noAuth{}
	}
	config.Init()
	mux := http.NewServeMux()
	metric := gmetric.New()
	if config.Config == nil {
		return nil, fmt.Errorf("gateway config was empty")
	}
	service, err := gateway.SingletonWithConfig(config.Config, registry.Codecs, registry.Types, metric)
	if err != nil {
		return nil, err
	}

	mux.Handle(config.Meta.MetricURI, auth.Auth(gmetric.NewHandler(config.Meta.MetricURI, metric).ServeHTTP))
	mux.Handle(config.Meta.ConfigURI, auth.Auth(handler.NewConfig(config.Config, &config.Endpoint, &config.Meta).ServeHTTP))
	mux.Handle(config.Meta.StatusURI, auth.Auth(handler.NewStatus(config.Version, &config.Meta).ServeHTTP))
	mux.Handle(config.Meta.ViewURI, auth.Auth(handler.NewView(config.Meta.ViewURI, &config.Meta, service.View).ServeHTTP))
	mux.Handle(config.Meta.OpenApiURI, auth.Auth(handler.NewOpenApi(config.Info, service.Routes).ServeHTTP))

	//actual datly handler
	mux.HandleFunc(config.Config.APIPrefix, auth.Auth(service.Handle))
	server := &Server{
		Server: http.Server{
			Addr:           ":" + strconv.Itoa(config.Endpoint.Port),
			Handler:        mux,
			ReadTimeout:    time.Millisecond * time.Duration(config.Endpoint.ReadTimeoutMs),
			WriteTimeout:   time.Millisecond * time.Duration(config.Endpoint.WriteTimeoutMs),
			MaxHeaderBytes: config.Endpoint.MaxHeaderBytes,
		},
	}
	server.shutdownOnInterrupt()
	return server, nil
}

func New(config *Config) (*Server, error) {
	return NewWithAuth(config, nil)
}
