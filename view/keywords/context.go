package keywords

type ContextMetadata struct {
	ContextName string
	Metadata    interface{}
}

func NewContextMetadata(name string, metadata interface{}) *ContextMetadata {
	return &ContextMetadata{
		ContextName: name,
		Metadata:    metadata,
	}
}
