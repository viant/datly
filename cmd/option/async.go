package option

type AsyncConfig struct {
	PrincipalSubject string `json:",omitempty" yaml:",omitempty"`
	Connector        string `json:",omitempty" yaml:",omitempty"`
	EnsureTable      *bool  `json:",omitempty" yaml:",omitempty"`
	ExpiryTimeInS    int    `json:",omitempty" yaml:",omitempty"`
	MarshalRelations *bool  `json:",omitempty" yaml:",omitempty"`
	Dataset          string `json:",omitempty" yaml:",omitempty"`
	BucketURL        string `json:",omitempty" yaml:",omitempty"`
}
