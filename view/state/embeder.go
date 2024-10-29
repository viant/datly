package state

import (
	"embed"
	"reflect"
)

// Embedder represents embedder
type Embedder interface {
	EmbedFS() *embed.FS
}

// FSEmbedder represents fs embedder
type FSEmbedder struct {
	fs       *embed.FS
	embedder Embedder
	rType    reflect.Type
}

// EmbedFS returns embed fs
func (f *FSEmbedder) EmbedFS() *embed.FS {
	if f == nil {
		return nil
	}
	if f.embedder != nil {
		return f.embedder.EmbedFS()
	}
	return f.fs
}

// Init initializes embedder
func (f *FSEmbedder) init() error {
	if f.rType == nil {
		return nil
	}
	instance := reflect.New(f.rType).Elem().Interface()
	if embedder, ok := instance.(Embedder); ok {
		f.embedder = embedder
	}
	return nil
}

// SetType sets type
func (f *FSEmbedder) SetType(rType reflect.Type) {
	f.rType = rType
	_ = f.init()
}

func NewFSEmbedder(fs *embed.FS) *FSEmbedder {
	return &FSEmbedder{fs: fs}
}
