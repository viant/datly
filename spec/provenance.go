package spec

type Provenance struct {
	Source   string `json:"source,omitempty"`
	Path     string `json:"path,omitempty"`
	Line     int    `json:"line,omitempty"`
	Column   int    `json:"column,omitempty"`
	Imported bool   `json:"imported,omitempty"`
}
