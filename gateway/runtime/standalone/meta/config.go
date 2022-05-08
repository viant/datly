package meta

const (
	//MetricURI represents default metric URI
	MetricURI = "/v1/api/metric/"
	//StatusURI represents status URI
	StatusURI = "/v1/api/status"
	//ConfigURI represents default config URI
	ConfigURI = "/v1/api/config"
)

//Config represents meta config
type Config struct {
	Version       string
	MetricURI     string
	ConfigURI     string
	StatusURI     string
	AllowedSubnet []string
}

//Init initialises config
func (m *Config) Init() {
	if m.MetricURI == "" {
		m.MetricURI = MetricURI
	}
	if m.StatusURI == "" {
		m.StatusURI = StatusURI
	}
	if m.ConfigURI == "" {
		m.ConfigURI = ConfigURI
	}
}
