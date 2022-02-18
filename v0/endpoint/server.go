package endpoint

import (
	"context"
	"fmt"
	"github.com/viant/datly/v0/shared"
	"log"
	"net/http"
	"os"
	"os/signal"
)

type Server struct {
	http.Server
	http.Handler
	port int
}

//ListenAndServe start http endpoint
func (r *Server) ListenAndServe() error {
	log.Printf("starting endpoint: %v", r.port)
	return r.Server.ListenAndServe()
}

func (r *Server) Shutdown(ctx context.Context) error {
	return r.Server.Shutdown(ctx)
}

//ShutdownOnInterrupt
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

//New creates a new server
func New(port int, handlers map[string]shared.Handle) *Server {
	mux := http.NewServeMux()
	for k, v := range handlers {
		mux.HandleFunc(k, v)
	}
	result := &Server{port: port}
	result.Server.Addr = ":" + fmt.Sprintf("%d", port)
	result.Server.Handler = mux
	result.shutdownOnInterrupt()
	return result
}
