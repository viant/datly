package boos

import (
	"context"
	"fmt"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xdatly/types/core"
	"github.com/viant/xdatly/types/custom/checksum"
	"net/http"
	"reflect"
	"strings"
)

const prefixInclude = "INCLUDE_"

func init() {
	core.RegisterType(PackageName, "BoosQueryBuilder", reflect.TypeOf(BoosQueryBuilder{}), checksum.GeneratedTime)
}

type BoosQueryBuilder struct {
}

func (b *BoosQueryBuilder) BuildCriteria(ctx context.Context, value interface{}, options *codec.CriteriaBuilderOptions) (*codec.Criteria, error) {
	asRequest, ok := value.(*http.Request)
	if !ok {
		return nil, fmt.Errorf("expected value to be type of %T but was %T", asRequest, value)
	}

	params := asRequest.URL.Query()

	query := &strings.Builder{}
	var args []interface{}

	for paramName, paramValue := range params {
		if !strings.HasPrefix(paramName, prefixInclude) {
			continue
		}

		columnKey := paramName[len(prefixInclude):]
		column, ok := options.Columns.Column(columnKey)
		if !ok {
			return nil, fmt.Errorf("not found column %v", columnKey)
		}

		if query.Len() != 0 {
			query.WriteString(" AND ")
		}

		query.WriteString(column.ColumnName())
		query.WriteString(" = ?")
		args = append(args, paramValue[0])
	}

	return &codec.Criteria{
		Query: query.String(),
		Args:  args,
	}, nil
}
