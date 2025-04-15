package extension

type Integration struct {
	Resources
	ResourcesByURI map[string]*Resource
	ResourceTemplates
	ResourceTemplatesByURI map[string]*ResourceTemplate
	Tools
	ToolsByName map[string]*Tool
}

func (i *Integration) AddResourceTemplate(resourceTemplate *ResourceTemplate) {
	// Add a resource template to the integration
	i.ResourceTemplates = append(i.ResourceTemplates, resourceTemplate)
	i.ResourceTemplatesByURI[resourceTemplate.UriTemplate] = resourceTemplate
}

func (i *Integration) AddResource(resource *Resource) {
	// Add a resource to the integration
	i.Resources = append(i.Resources, resource)
	i.ResourcesByURI[resource.Uri] = resource
}

func (i *Integration) AddTool(tool *Tool) {
	// Add a tool to the integration
	i.Tools = append(i.Tools, tool)
	i.ToolsByName[tool.Name] = tool
}

func NewIntegration() *Integration {
	// constructor for Integration
	return &Integration{
		Resources:              make(Resources, 0),
		ResourceTemplates:      make(ResourceTemplates, 0),
		Tools:                  make(Tools, 0),
		ResourcesByURI:         make(map[string]*Resource),
		ResourceTemplatesByURI: make(map[string]*ResourceTemplate),
		ToolsByName:            make(map[string]*Tool),
	}
}
