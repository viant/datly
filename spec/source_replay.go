package spec

import "strings"

// RequestValue classifies durable values using canonical transport metadata.
// The invocation's HTTP request itself is reconstructed, never persisted.
func (s BindSource) RequestValue() bool {
	return strings.ToLower(strings.TrimSpace(s.Kind)) != "http_request" && (&Parameter{Source: s}).IsTransportInput()
}
