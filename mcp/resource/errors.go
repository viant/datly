package resource

import "errors"

var (
	ErrInvalidURI = errors.New("invalid MCP resource URI")
	ErrNotFound   = errors.New("MCP resource not found")
	ErrAmbiguous  = errors.New("ambiguous MCP resource URI")
)
