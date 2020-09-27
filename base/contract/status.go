package contract

import (
	"github.com/viant/datly/config"
	"github.com/viant/datly/metric"
	"github.com/viant/datly/shared"
	"strings"
	"sync"
	"time"
)

//StatusInfo represents status
type StatusInfo struct {
	JobID           string `json:",omitempty"`
	Status          string
	Errors          []*ErrorInfo    `json:",omitempty"`
	Metrics         *metric.Metrics `json:",omitempty"`
	RuleURL         string          `json:",omitempty"`
	Rule            *config.Rule    `json:",omitempty"`
	Rules           int             `json:",omitempty"`
	ServiceTimeMs   int             `json:",omitempty"`
	ExecutionTimeMs int             `json:",omitempty"`
	CreateTime      time.Time       `json:",omitempty"`
	StartTime       time.Time       `json:",omitempty"`
	mux             sync.Mutex
}

//Clone clones status info
func (i StatusInfo) Clone() *StatusInfo {
	result := &StatusInfo{
		JobID:           i.JobID,
		Status:          i.Status,
		Errors:          i.Errors,
		Metrics:         i.Metrics.Clone(),
		RuleURL:         i.RuleURL,
		Rule:            i.Rule,
		Rules:           i.Rules,
		ServiceTimeMs:   i.ServiceTimeMs,
		CreateTime:      i.CreateTime,
		ExecutionTimeMs: i.ExecutionTimeMs,
		StartTime:       i.StartTime,
		mux:             sync.Mutex{},
	}
	return result
}

//ApplyFilter applies info filter
func (i *StatusInfo) ApplyFilter(metrics string) *StatusInfo {
	result := i.Clone()
	if i.Metrics != nil {
		switch strings.ToLower(metrics) {
		case shared.MetricsAll:
			i.Metrics.IncludeSQL()
		case "":
			i.Metrics = nil
		default:
		}
	}
	i.Rule = nil
	result.Rule = nil
	return result
}

//OnDone computes time taken
func (i *StatusInfo) OnDone() {
	i.ExecutionTimeMs = int(time.Now().Sub(i.StartTime) / time.Millisecond)
	i.ServiceTimeMs = int(time.Now().Sub(i.CreateTime) / time.Millisecond)

}

func NewStatusInfo() StatusInfo {
	return StatusInfo{
		StartTime:  time.Now(),
		CreateTime: time.Now(),
		Metrics:    metric.NewMetrics(),
		mux:        sync.Mutex{},
		Status:     shared.StatusOK,
	}
}

//ErrorInfo represents an error info
type ErrorInfo struct {
	Message  string
	Location string
	Type     string
}

//AddError add an error to response
func (i *StatusInfo) AddError(errType, location string, err error) {
	if err == nil {
		return
	}
	i.mux.Lock()
	defer i.mux.Unlock()
	switch errType {
	case shared.ErrorTypeCache:

	default:
		i.Status = shared.StatusError
	}
	if len(i.Errors) == 0 {
		i.Errors = make([]*ErrorInfo, 0)
	}
	info := &ErrorInfo{
		Location: location,
		Type:     errType,
		Message:  err.Error(),
	}
	i.Errors = append(i.Errors, info)
}
