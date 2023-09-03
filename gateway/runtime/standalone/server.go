package standalone

import (
	"context"
	"fmt"
	"github.com/viant/datly/config"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/gateway/router"
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
	Service *gateway.Service
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

func (r *Server) Routes() []*router.Route {
	aRouter, ok := r.Service.Router()
	if !ok {
		return []*router.Route{}
	}

	return aRouter.MatchAllByPrefix("")
}

func NewWithAuth(gwayConfig *Config, auth gateway.Authorizer) (*Server, error) {
	gwayConfig.Init()
	//mux := http.NewServeMux()
	metric := gmetric.New()
	if gwayConfig.Config == nil {
		return nil, fmt.Errorf("gateway config was empty")
	}

	service, err := gateway.SingletonWithConfig(
		gwayConfig.Config,
		handler.NewStatus(gwayConfig.Version, &gwayConfig.Meta),
		auth,
		config.Config,
		metric,
	)

	if err != nil {
		if service != nil {
			_ = service.Close()
		}

		return nil, err
	}

	server := &Server{
		Service: service,
		Server: http.Server{
			Addr:           ":" + strconv.Itoa(gwayConfig.Endpoint.Port),
			Handler:        service,
			ReadTimeout:    time.Millisecond * time.Duration(gwayConfig.Endpoint.ReadTimeoutMs),
			WriteTimeout:   time.Millisecond * time.Duration(gwayConfig.Endpoint.WriteTimeoutMs),
			MaxHeaderBytes: gwayConfig.Endpoint.MaxHeaderBytes,
		},
	}

	server.shutdownOnInterrupt()
	return server, nil
}

func New(config *Config) (*Server, error) {
	return NewWithAuth(config, nil)
}
