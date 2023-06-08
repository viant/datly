package session

import (
	"context"
	"github.com/viant/godiff"
	"github.com/viant/xdatly/handler/differ"
	"reflect"
)

var differRegistry = godiff.NewRegistry()

type Differ struct {
}

func (d *Differ) Diff(ctx context.Context, from, to interface{}, opts ...differ.Option) *differ.ChangeLog {

	var diffOptions []godiff.Option
	options := differ.Options{}
	options.Apply(opts...)
	if options.WithShallow {
		diffOptions = append(diffOptions, godiff.WithShallow(true))
	}
	if options.WithSetMarker {
		diffOptions = append(diffOptions, godiff.WithSetMarker(true))
	}
	aDiffer, err := differRegistry.Get(reflect.TypeOf(from), reflect.TypeOf(to), &godiff.Tag{})
	if err != nil {
		return nil
	}
	diff := aDiffer.Diff(from, to, diffOptions...)
	var result = differ.ChangeLog{}
	if len(diff.Changes) > 0 {
		for _, item := range diff.Changes {
			//	aPath := differ.Path(*item.Path)
			aChange := differ.Change{
				Type:  differ.ChangeType(item.Type),
				Path:  nil,
				From:  item.From,
				To:    item.To,
				Error: item.Error,
			}
			result.Changes = append(result.Changes, &aChange)
		}
	}
	return &result
}
