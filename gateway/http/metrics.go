package http

import "net/http"

// MetricsConfig is operator policy for client-requested diagnostic headers.
// Configuring it enables SQL-redacted metrics; AllowSQL separately enables SQL
// and arguments for debug requests. Authorize can further restrict each caller.
type MetricsConfig struct {
	AllowSQL  bool                      `json:"AllowSQL,omitempty" yaml:"AllowSQL,omitempty"`
	Authorize func(*http.Request) error `json:"-" yaml:"-"`
}
