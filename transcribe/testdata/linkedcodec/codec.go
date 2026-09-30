package linkedcodec

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	xcodec "github.com/viant/xdatly/codec"
)

type QueryList struct{}
type queryList struct{}

func (*QueryList) New(config *xcodec.Config, options ...xcodec.Option) (xcodec.Instance, error) {
	if config.SourceType != reflect.TypeFor[[]string]() || config.DestinationType != reflect.TypeFor[[]int]() {
		return nil, fmt.Errorf("QueryList requires []string to []int")
	}
	if len(config.Args) > 0 && (len(config.Args) != 1 || config.Args[0] != "strict") {
		return nil, fmt.Errorf("invalid QueryList mode")
	}
	if xcodec.NewOptions(options).LookupType == nil {
		return nil, fmt.Errorf("QueryList requires type lookup")
	}
	return &queryList{}, nil
}

func (*queryList) Value(_ context.Context, value any, _ ...xcodec.Option) (any, error) {
	var result []int
	for _, item := range value.([]string) {
		for _, text := range strings.Split(item, ",") {
			number, err := strconv.Atoi(text)
			if err != nil {
				return nil, err
			}
			result = append(result, number)
		}
	}
	return result, nil
}

type Upper struct{}

func (*Upper) Value(_ context.Context, value any, _ ...xcodec.Option) (any, error) {
	return strings.ToUpper(value.(string)), nil
}

type WithSuffix struct{}

func (*WithSuffix) Value(ctx context.Context, value any, options ...xcodec.Option) (any, error) {
	lookup := xcodec.NewOptions(options).LookupValue
	if lookup == nil {
		return nil, fmt.Errorf("invocation value lookup was lost")
	}
	suffix, err := lookup(ctx, "Suffix")
	if err != nil {
		return nil, err
	}
	return strings.ToUpper(value.(string)) + suffix.(string), nil
}

// A sibling must not be imported merely because QueryList is referenced.
type Unrelated struct{}

var QueryListType = reflect.TypeFor[QueryList]()
var UpperType = reflect.TypeFor[Upper]()
var UnrelatedType = reflect.TypeFor[Unrelated]()
var WithSuffixType = reflect.TypeFor[WithSuffix]()

func init() {}
