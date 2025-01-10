package function

import (
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
)

type publishParent struct{}

func (c *publishParent) Apply(args []string, column *sqlparser.Column, resource *view.Resource, aView *view.View) error {
	aView.PublishParent = true
	return nil
}

func (c *publishParent) Name() string {
	return "publish_parent"
}

func (c *publishParent) Description() string {
	return "set view.PublishParent to pass parent in hooks by context"
}

func (c *publishParent) Arguments() []*Argument {
	return []*Argument{}
}
