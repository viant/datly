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
	var instance interface{}
	if f.rType.Kind() == reflect.Pointer {
		instance = reflect.New(f.rType).Elem().Interface()
	} else {
		instance = reflect.New(f.rType).Interface()
	}
	if embedder, ok := instance.(Embedder); ok {
		f.embedder = embedder
	}
	return nil
}

// SetType sets type
func (f *FSEmbedder) SetType(rType reflect.Type) bool {
	f.rType = rType
	_ = f.init()
	return f.embedder != nil
}

func NewFSEmbedder(fs *embed.FS) *FSEmbedder {
	return &FSEmbedder{fs: fs}
}
