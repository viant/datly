package spec

import (
	"fmt"
	"strings"
)

// ResponseCompression is an opt-in HTTP policy applied after output encoding.
// MinSizeBytes is a strict lower bound: equal-sized bodies remain uncompressed.
// Explicit response objects own their streams and encoding independently.
type ResponseCompression struct {
	Encoding     string `json:"encoding"`
	MinSizeBytes int    `json:"minSizeBytes"`
}

func (c *ResponseCompression) Validate() error {
	if c == nil {
		return nil
	}
	if strings.TrimSpace(c.Encoding) != "gzip" {
		return fmt.Errorf("unsupported response compression encoding %q; expected gzip", c.Encoding)
	}
	if c.MinSizeBytes < 0 {
		return fmt.Errorf("response compression minSizeBytes must be nonnegative")
	}
	return nil
}
func (c *ResponseCompression) Clone() *ResponseCompression {
	if c == nil {
		return nil
	}
	result := *c
	return &result
}
