package state

import "embed"

type Embedder interface {
	FS() *embed.FS
}
