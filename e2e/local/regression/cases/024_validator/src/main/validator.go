package main

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"
)

func main() {
	port := os.Getenv("VALIDATOR_PORT")
	if port == "" {
		port = "8081"
	}

	h := &server{
		server: &http.Server{
			Addr:    "localhost:" + port,
			Handler: &handler{},
		},
	}
	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt)
		<-sigint
		// We received an interrupt signal, shut down.
		if err := h.Shutdown(context.Background()); err != nil {
			// Error from closing listeners, or context timeout:
			log.Printf("HTTP server Shutdown: %v", err)
		}
	}()

	h.server.ListenAndServe()
}

type (
	server struct {
		server  *http.Server
		handler *handler
	}

	EventPerfValidation struct {
		Price     float64
		Timestamp time.Time
	}

	handler struct {
	}
)

func (h *handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if strings.HasSuffix(request.RequestURI, "/dev/validate/event-perf") {
		status, payload := h.validateEventPerfs(request)
		writer.WriteHeader(status)
		writer.Write(payload)
		return
	}

	writer.WriteHeader(http.StatusNotFound)
}

func (h *handler) validateEventPerfs(request *http.Request) (int, []byte) {
	all, err := ioutil.ReadAll(request.Body)
	defer request.Body.Close()
	if err != nil {
		return http.StatusInternalServerError, nil
	}

	var data []*EventPerfValidation
	if err = json.Unmarshal(all, &data); err != nil {
		return http.StatusInternalServerError, nil
	}

	for _, datum := range data {
		if datum.Price < 0 {
			marshal, _ := json.Marshal(&Response{Message: "price can't be negative"})
			return http.StatusBadRequest, marshal
		}
	}

	return http.StatusOK, nil
}

func (h *server) Shutdown(background context.Context) error {
	return h.server.Close()
}

type Response struct {
	Message string
}
