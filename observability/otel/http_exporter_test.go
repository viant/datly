package otel

import (
	"context"
	"errors"
	"testing"
)

func TestHTTPExporterExplicitConfiguration(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://ambient.example")
	for _, config := range []HTTPExporter{{}, {EndpointURL: "http://localhost/v1/traces"}, {EndpointURL: "https://user:secret@localhost/v1/traces"}, {EndpointURL: "https://localhost/v1/traces?secret=hidden"}, {EndpointURL: "https://localhost/v1/traces", Headers: map[string]string{"Authorization": "bad\nvalue"}}, {EndpointURL: "https://localhost/v1/traces", Headers: map[string]string{"Content-Type": "wrong"}}} {
		if err := config.Validate(); err == nil {
			t.Fatal("invalid exporter config accepted")
		}
	}
	c := HTTPExporter{EndpointURL: "http://127.0.0.1:4318/v1/traces", Insecure: true}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.New(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
