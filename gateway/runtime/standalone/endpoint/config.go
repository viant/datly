package endpoint

// Config defines standalone app endpoint
type Config struct {
	Port           int
	ReadTimeoutMs  int
	WriteTimeoutMs int
	MaxHeaderBytes int
}

// init initialises endpoint
func (e *Config) Init() {
	if e.Port == 0 {
		e.Port = 8080
	}
}
