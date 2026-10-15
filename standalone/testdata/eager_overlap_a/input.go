package eageroverlap

import "embed"

//go:embed query.sql
var resources embed.FS

// Input supplies one source-local SQL bundle for the standalone regression.
type Input struct{}

func (Input) EmbedFS() *embed.FS { return &resources }
