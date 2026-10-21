package typectx

// Import describes one package alias import for DQL/type resolution.
type Import struct {
	Alias   string `json:",omitempty" yaml:",omitempty"`
	Package string `json:",omitempty" yaml:",omitempty"`
}

// Context captures default package and imports used for type resolution.
type Context struct {
	DefaultPackage string   `json:",omitempty" yaml:",omitempty"`
	Imports        []Import `json:",omitempty" yaml:",omitempty"`
	PackageDir     string   `json:",omitempty" yaml:",omitempty"`
	PackageName    string   `json:",omitempty" yaml:",omitempty"`
	PackagePath    string   `json:",omitempty" yaml:",omitempty"`
}

// Provenance tracks where a resolved type came from.
type Provenance struct {
	Package string `json:",omitempty" yaml:",omitempty"`
	File    string `json:",omitempty" yaml:",omitempty"`
	Kind    string `json:",omitempty" yaml:",omitempty"` // builtin, resource_type, registry, ast_type
}

// Resolution captures one resolved type expression and its provenance.
type Resolution struct {
	Expression  string     `json:",omitempty" yaml:",omitempty"`
	Target      string     `json:",omitempty" yaml:",omitempty"`
	ResolvedKey string     `json:",omitempty" yaml:",omitempty"`
	MatchKind   string     `json:",omitempty" yaml:",omitempty"` // exact, alias_import, qualified, default_package, import_package, global_unique
	Provenance  Provenance `json:",omitempty" yaml:",omitempty"`
}
