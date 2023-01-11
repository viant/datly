package xdatly

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

type AsInts struct {
}

func (i *AsInts) Value(ctx context.Context, raw interface{}, options ...interface{}) (interface{}, error) {
	asString, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("unexpected parameter value type, wanted %T, got %T", asString, raw)
	}

	split := strings.Split(asString, ",")
	result := make([]int, len(split))

	var err error
	for index, segment := range split {
		result[index], err = strconv.Atoi(segment)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}
