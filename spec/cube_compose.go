package spec

// CubeComposeSettings enables a bounded, request-local composition of cube queries.
type CubeComposeSettings struct {
	Enabled   bool  `json:"enabled,omitempty"`
	MCPTool   *bool `json:"mcpTool,omitempty"`
	MaxCubes  int   `json:"maxCubes,omitempty"`
	MaxLimit  int   `json:"maxLimit,omitempty"`
	TimeoutMs int   `json:"timeoutMs,omitempty"`
}

func (s *CubeComposeSettings) Clone() *CubeComposeSettings {
	if s == nil {
		return nil
	}
	result := *s
	if s.MCPTool != nil {
		value := *s.MCPTool
		result.MCPTool = &value
	}
	return &result
}

func (s *CubeComposeSettings) Normalize() *CubeComposeSettings {
	result := s.Clone()
	if result == nil {
		return nil
	}
	if result.MaxCubes <= 0 {
		result.MaxCubes = 8
	}
	if result.MaxLimit <= 0 {
		result.MaxLimit = 100
	}
	if result.TimeoutMs <= 0 {
		result.TimeoutMs = 30000
	}
	return result
}
