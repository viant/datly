package router

import (
	"fmt"
	"github.com/viant/datly/oas/openapi3"
	"github.com/viant/ptrie"
)

type Router struct {
	trie ptrie.Trie
}


//New creates a new router
func New(paths openapi3.Paths) (*Router, error) {
	router := &Router{
		trie: ptrie.New(),
	}
	for aPath, v := range paths {
		index := _terminators.Index(aPath)
		key := aPath[:index]
		pathRoute := &route{path: aPath, item: v}
		pathRoute.Init()
		err := pathRoute.Validate()
		if err != nil {
			return nil, fmt.Errorf("invalid path: %v, %w", aPath, err)
		}
		if err := router.trie.Put([]byte(key), pathRoute); err != nil {
			return nil, fmt.Errorf("failed to process path: %v, due to: %w", aPath, err)
		}

	}
	return router, nil
}

