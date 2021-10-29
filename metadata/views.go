package metadata

import (
	"fmt"
)

type Views []*View

//View returns a view for supplied name or error
func (v Views) View(name string) (*View, error) {
	if len(v) == 0 {
		return nil, fmt.Errorf("failed to lookup view: %v", name)
	}
	for _, view := range v {
		if view.Name == name {
			return view, nil
		}
	}
	return nil, fmt.Errorf("failed to lookup view: %v", name)
}

//Connector sets connector
func (v Views) Connector(name string) {
	for i := range v {
		v[i].Connector = name
	}
}
