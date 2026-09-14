package engine

import "context"

// A child may open the shared transaction with a merged call context. Retain
// that context until root completion; canceling it at child return would make
// database/sql roll back the parent's still-pending transaction.
func (s *dataScope) retainContext(cancel context.CancelFunc) {
	root := s.root
	if root == nil {
		root = s
	}
	root.mu.Lock()
	root.contextReleases = append(root.contextReleases, cancel)
	root.mu.Unlock()
}
func (s *dataScope) releaseContexts() {
	s.mu.Lock()
	releases := s.contextReleases
	s.contextReleases = nil
	s.mu.Unlock()
	for _, release := range releases {
		release()
	}
}
