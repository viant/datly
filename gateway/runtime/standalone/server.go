package standalone

import (
	"context"
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

func New(config *Config) (*Server, error) {
	config.Init()
	mux := http.NewServeMux()
	metric := gmetric.New()
	service, err := gateway.SingletonWithConfig(config.Gateway, registry.Codecs, registry.Types, metric)
	if err != nil {
		return nil, err
	}
	mux.Handle(config.Meta.MetricURI, gmetric.NewHandler(config.Meta.MetricURI, metric))
	mux.Handle(config.Meta.ConfigURI, handler.NewConfig(config.Gateway, &config.Endpoint, config.Meta))
	mux.Handle(config.Meta.StatusURI, handler.NewStatus(config.Version, config.Meta))

	//actual datly handler
	mux.HandleFunc(config.Gateway.APIPrefix, service.Handle)
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
