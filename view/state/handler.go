package state

// Handler represents a handler
type Handler struct {
	Name string   `tag:"name,omitempty"`
	Args []string `tag:"arguments,omitempty"`
}
