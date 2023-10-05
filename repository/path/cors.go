package path

type Cors struct {
	AllowCredentials *bool     `yaml:"AllowCredentials,omitempty"`
	AllowHeaders     *[]string `yaml:"AllowHeaders,omitempty"`
	AllowMethods     *[]string `yaml:"AllowMethods,omitempty"`
	AllowOrigins     *[]string `yaml:"AllowOrigins,omitempty"`
	ExposeHeaders    *[]string `yaml:"ExposeHeaders,omitempty"`
	MaxAge           *int64    `yaml:"MaxAge,omitempty"`
}

func (c *Cors) inherit(cors *Cors) {
	if cors == nil {
		return
	}

	if c.ExposeHeaders == nil {
		c.ExposeHeaders = cors.ExposeHeaders
	}

	if c.AllowMethods == nil {
		c.AllowMethods = cors.AllowMethods
	}

	if c.AllowHeaders == nil {
		c.AllowHeaders = cors.AllowHeaders
	}

	if c.AllowOrigins == nil {
		c.AllowOrigins = cors.AllowOrigins
	}

	if c.AllowCredentials == nil {
		c.AllowCredentials = cors.AllowCredentials
	}

	if c.MaxAge == nil {
		c.MaxAge = cors.MaxAge
	}
}
