package keywords

type (
	ContextMetadata struct {
		ContextName string
		Metadata    interface{}
	}

	FunctionMetadata struct {
	}
)

func NewContextMetadata(name string, metadata interface{}) *ContextMetadata {
	return &ContextMetadata{
		ContextName: name,
		Metadata:    metadata,
	}
}
