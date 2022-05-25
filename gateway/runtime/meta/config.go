package meta

const (
	//MetricURI represents default metric URIPrefix
	MetricURI = "/v1/api/meta/metric/"
	//StatusURI represents status URIPrefix
	StatusURI = "/v1/api/meta/status"
	//ConfigURI represents default config URIPrefix
	ConfigURI = "/v1/api/meta/config"
	//ViewURI represents default config view URIPrefix
	ViewURI = "/v1/api/meta/view/"
)

//Config represents meta config
type Config struct {
	Version       string
	MetricURI     string
	ConfigURI     string
	StatusURI     string
	ViewURI       string
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
	if m.ViewURI == "" {
		m.ViewURI = ViewURI
	}
}
