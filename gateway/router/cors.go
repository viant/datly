package router

type Cors struct {
	AllowCredentials *bool
	AllowHeaders     *[]string
	AllowMethods     *[]string
	AllowOrigins     *[]string
	ExposeHeaders    *[]string
	MaxAge           *int64
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
