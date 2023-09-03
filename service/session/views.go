package session

import "sync"

type views struct {
	populatedView map[string]bool
	sync.Mutex
}

func (c *views) canPopulateView(name string) bool {
	c.Lock()
	defer c.Unlock()
	if _, ok := c.populatedView[name]; ok {
		return false
	}
	c.populatedView[name] = true
	return true
}

func newViews() *views {
	return &views{populatedView: make(map[string]bool)}
}
