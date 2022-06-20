package openapi3

//OpenAPI represents OpenAPI 3.0 document
type OpenAPI struct {
	OpenAPI      string                 `json:"openapi" yaml:"openapi"` // Required
	Components   Components             `json:"components,omitempty" yaml:"components,omitempty"`
	Info         *Info                  `json:"info" yaml:"info"`   // Required
	Paths        Paths                  `json:"paths" yaml:"paths"` // Required
	Security     SecurityRequirements   `json:"security,omitempty" yaml:"security,omitempty"`
	Servers      Servers                `json:"servers,omitempty" yaml:"servers,omitempty"`
	Tags         Tags                   `json:"tags,omitempty" yaml:"tags,omitempty"`
	ExternalDocs *ExternalDocumentation `json:"externalDocs,omitempty" yaml:"externalDocs,omitempty"`
}
