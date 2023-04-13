package keywords

type (
	ContextMetadata struct {
		ContextName string
		Metadata    interface{}
		UnexpandRaw bool
	}
)

func NewContextMetadata(name string, metadata interface{}, unexpandRaw bool) *ContextMetadata {
	return &ContextMetadata{
		ContextName: name,
		Metadata:    metadata,
		UnexpandRaw: unexpandRaw,
	}
}
