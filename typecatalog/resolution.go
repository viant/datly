package typecatalog

type PackageImport struct {
	Alias   string `json:",omitempty" yaml:",omitempty"`
	Package string `json:",omitempty" yaml:",omitempty"`
}

type ResolutionContext struct {
	DefaultPackage string          `json:",omitempty" yaml:",omitempty"`
	Imports        []PackageImport `json:",omitempty" yaml:",omitempty"`
	PackageDir     string          `json:",omitempty" yaml:",omitempty"`
	PackageName    string          `json:",omitempty" yaml:",omitempty"`
	PackagePath    string          `json:",omitempty" yaml:",omitempty"`
}

type Provenance struct {
	Package string `json:",omitempty" yaml:",omitempty"`
	File    string `json:",omitempty" yaml:",omitempty"`
	Kind    string `json:",omitempty" yaml:",omitempty"`
}

type Resolution struct {
	Expression  string     `json:",omitempty" yaml:",omitempty"`
	Target      string     `json:",omitempty" yaml:",omitempty"`
	ResolvedKey string     `json:",omitempty" yaml:",omitempty"`
	MatchKind   string     `json:",omitempty" yaml:",omitempty"`
	Provenance  Provenance `json:",omitempty" yaml:",omitempty"`
}
