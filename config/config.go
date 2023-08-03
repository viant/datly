package config

import "reflect"

type (
	CodecConfig struct {
		Body       string
		ParamType  reflect.Type
		Args       []string
		OutputType string
	}
)
