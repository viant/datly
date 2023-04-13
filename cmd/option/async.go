package option

type AsyncConfig struct {
	TableName     string `json:",omitempty" yaml:",omitempty"`
	Qualifier     string `json:",omitempty" yaml:",omitempty"`
	Connector     string `json:",omitempty" yaml:",omitempty"`
	EnsureTable   *bool  `json:",omitempty" yaml:",omitempty"`
	ExpiryTimeInS int    `json:",omitempty" yaml:",omitempty"`
}
