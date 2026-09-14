package report

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"

	xcodec "github.com/viant/xdatly/codec"
)

const groupedCSVCodecName = "report_csv_ints"

type groupedSpendInput struct {
	AccountIDs []int
	Tenant     string
	Region     string
	Channel    string
	Status     string
}

type groupedSpendDetail struct {
	Tenant    string `sqlx:"tenant"`
	AccountID int    `sqlx:"account_id"`
	Label     string `sqlx:"label"`
}

type groupedSpendRow struct {
	Tenant     string                `sqlx:"tenant"`
	AccountID  int                   `sqlx:"account_id"`
	Region     string                `sqlx:"region"`
	TotalSpend float64               `sqlx:"total_spend"`
	OrderCount int                   `sqlx:"order_count"`
	Details    []*groupedSpendDetail `sqlx:"-"`
}

type groupedSpendOutput struct {
	Rows []*groupedSpendRow
}

type groupedLinkedDimensions struct {
	Tenant    bool `json:"tenant,omitempty"`
	AccountID bool `json:"accountID,omitempty"`
	Region    bool `json:"region,omitempty"`
}

type groupedLinkedMeasures struct {
	TotalSpend bool `json:"totalSpend,omitempty"`
	OrderCount bool `json:"orderCount,omitempty"`
}

type groupedLinkedFilters struct {
	AccountIDs *string `json:"accountIDs,omitempty"`
	Tenant     *string `json:"tenant,omitempty"`
	Region     *string `json:"region,omitempty"`
	Channel    *string `json:"channel,omitempty"`
	Status     *string `json:"status,omitempty"`
}

type groupedLinkedInput struct {
	Dimensions groupedLinkedDimensions `json:"dimensions,omitempty"`
	Measures   groupedLinkedMeasures   `json:"measures,omitempty"`
	Filters    groupedLinkedFilters    `json:"filters,omitempty"`
	OrderBy    []string                `json:"orderBy,omitempty"`
	Limit      *int                    `json:"limit,omitempty"`
	Offset     *int                    `json:"offset,omitempty"`
}

type groupedReportFilters struct {
	AccountIDs string
	Tenant     string
	Region     string
	Channel    string
	Status     string
}

type groupedReportRequest struct {
	Dimensions []string
	Measures   []string
	Filters    groupedReportFilters
	OrderBy    []string
	Limit      *int
	Offset     *int
}

type groupedFilterValue struct {
	name  string
	value string
}

type groupedCSVCodecFactory struct {
	calls atomic.Int32
}

func (f *groupedCSVCodecFactory) New(config *xcodec.Config, _ ...xcodec.Option) (xcodec.Instance, error) {
	if config == nil || config.Body != groupedCSVCodecName {
		return nil, fmt.Errorf("unsupported grouped report codec")
	}
	if config.SourceType != reflect.TypeOf("") || config.DestinationType != reflect.TypeOf([]int{}) {
		return nil, fmt.Errorf("grouped report codec requires string to []int, got %v to %v", config.SourceType, config.DestinationType)
	}
	return groupedCSVCodec{calls: &f.calls}, nil
}

type groupedCSVCodec struct {
	calls *atomic.Int32
}

func (c groupedCSVCodec) Value(_ context.Context, raw interface{}, _ ...xcodec.Option) (interface{}, error) {
	c.calls.Add(1)
	text, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("grouped report codec requires string, got %T", raw)
	}
	parts := strings.Split(text, ",")
	result := make([]int, 0, len(parts))
	for _, part := range parts {
		value, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil {
			return nil, fmt.Errorf("parse account ID %q: %w", part, err)
		}
		result = append(result, value)
	}
	return result, nil
}
