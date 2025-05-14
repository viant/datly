package path

import (
	"github.com/viant/datly/internal/setter"
)

type Cors struct {
	AllowCredentials *bool     `yaml:"AllowCredentials,omitempty"`
	AllowHeaders     *[]string `yaml:"AllowHeaders,omitempty"`
	AllowMethods     *[]string `yaml:"AllowMethods,omitempty"`
	AllowOrigins     *[]string `yaml:"AllowOrigins,omitempty"`
	ExposeHeaders    *[]string `yaml:"ExposeHeaders,omitempty"`
	MaxAge           *int64    `yaml:"MaxAge,omitempty"`
}

func (c *Cors) Inherit(cors *Cors) {
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

	if c.AllowCredentials != nil && *c.AllowCredentials {
		if c.AllowOrigins != nil && len(*c.AllowOrigins) == 1 && (*c.AllowOrigins)[0] == "*" {
			c.AllowOrigins = cors.AllowOrigins
		}
	}

	if c.MaxAge == nil {
		c.MaxAge = cors.MaxAge
	}
}

func (c *Cors) OriginMap() map[string]bool {
	var result = make(map[string]bool)
	if c.AllowOrigins != nil {
		for _, origin := range *c.AllowOrigins {
			result[origin] = true
		}
	}
	return result
}

func DefaultCors() *Cors {
	ret := &Cors{
		AllowCredentials: setter.BoolPtr(true),
		AllowHeaders:     setter.StringsPtr("*"),
		AllowMethods:     setter.StringsPtr("*"),
		AllowOrigins:     setter.StringsPtr("*"),
		ExposeHeaders:    setter.StringsPtr("*"),
	}
	return ret
}
