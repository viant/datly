package warmup

import (
	"context"
	"fmt"
	"github.com/viant/datly/internal/gmetricx"
	"github.com/viant/datly/view"
	"github.com/viant/datly/warmup"
	"github.com/viant/gmetric"
	"github.com/viant/gmetric/counter/base"
	"net/http"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/viant/afs/url"
)

const (
	warmupRunOKKey            = "run.ok"
	warmupRunErrorKey         = "run.error"
	warmupCasesCompletedKey   = "cases.completed"
	warmupCasesFailedKey      = "cases.failed"
	warmupRowsKey             = "rows"
	warmupMetricFallbackPkg   = "datly"
	warmupMetricRecentBuckets = 2
)

type PreCachables func(ctx context.Context, method, matchingURI string) ([]*view.View, error)
type PreCached struct {
	URI        string
	View       string
	Column     string
	Params     string
	CacheKey   string
	FieldNames string `json:",omitempty"`
	Elapsed    string
	TimeTaken  time.Duration
	Rows       int
	Error      string `json:"error,omitempty"`
}

type Summary struct {
	CompletedCases int `json:"completedCases"`
	FailedCases    int `json:"failedCases"`
	WarmedRows     int `json:"warmedRows"`
}

type viewSummary struct {
	View           string
	CompletedCases int
	FailedCases    int
	WarmedRows     int
	Elapsed        time.Duration
}

type Response struct {
	Error     string       `json:"error,omitempty"`
	Status    string       `json:"status"`
	Summary   *Summary     `json:"summary,omitempty"`
	PreCached []*PreCached `json:"preCached"`
}

func PreCache(ctx context.Context, lookup PreCachables, warmupURIs ...string) *Response {
	started := time.Now()
	fmt.Printf("[INFO] cache warmup request start start_time=%s uris=%v\n", started.Format(time.RFC3339), warmupURIs)
	group := sync.WaitGroup{}
	var mux = sync.Mutex{}
	var response = &Response{Status: "ok"}
	var errors []string
	setErr := func(e error) {
		if e == nil {
			return
		}
		mux.Lock()
		defer mux.Unlock()
		errors = append(errors, e.Error())
	}

	for _, URI := range warmupURIs {
		group.Add(1)
		go func(URI string) {
			defer group.Done()
			startTime := time.Now()
			fmt.Printf("[INFO] cache warmup uri start uri=%s start_time=%s\n", URI, startTime.Format(time.RFC3339))
			views, e := lookup(ctx, http.MethodGet, URI)
			if e != nil {
				fmt.Printf("[INFO] cache warmup uri lookup error uri=%s elapsed=%s error=%v\n", URI, time.Since(startTime), e)
				setErr(e)
				mux.Lock()
				response.PreCached = append(response.PreCached, &PreCached{
					URI:       URI,
					Elapsed:   time.Since(startTime).String(),
					TimeTaken: time.Since(startTime),
					Error:     e.Error(),
				})
				mux.Unlock()
				return
			}
			fmt.Printf("[INFO] cache warmup uri views uri=%s count=%d views=%s elapsed=%s\n", URI, len(views), viewNames(views), time.Since(startTime))
			var result *warmup.Result
			if result, e = warmup.PopulateCacheWithDetailsContext(ctx, views); e != nil {
				fmt.Printf("[INFO] cache warmup uri populate error uri=%s elapsed=%s error=%v\n", URI, time.Since(startTime), e)
				setErr(e)
			}
			elapsed := time.Now().Sub(startTime)
			rows := 0
			if result != nil {
				rows = result.Rows
			}
			fmt.Printf("[INFO] cache warmup uri done uri=%s rows=%d elapsed=%s\n", URI, rows, elapsed)
			if result == nil {
				return
			}
			logViewSummaries(URI, views, result)
			mux.Lock()
			appendPreCached(response, URI, result)
			mux.Unlock()
		}(URI)
	}
	group.Wait()
	response.Summary = summarize(response.PreCached)
	if len(errors) > 0 {
		response.Error = joinErrors(errors)
		response.Status = "error"
	}
	fmt.Printf("[INFO] cache warmup request done status=%s elapsed=%s\n", response.Status, time.Since(started))
	return response
}

func appendPreCached(response *Response, URI string, result *warmup.Result) {
	if response == nil || result == nil {
		return
	}
	for _, entry := range result.Entries {
		if entry == nil {
			continue
		}
		response.PreCached = append(response.PreCached, &PreCached{
			URI:        URI,
			View:       entry.View,
			Column:     entry.Column,
			Params:     entry.Params,
			CacheKey:   entry.CacheKey,
			FieldNames: entry.FieldNames,
			Elapsed:    entry.Elapsed,
			TimeTaken:  entry.TimeTaken,
			Rows:       entry.Rows,
			Error:      entry.Error,
		})
	}
}

func summarize(entries []*PreCached) *Summary {
	summary := &Summary{}
	if len(entries) == 0 {
		return summary
	}
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		if entry.Error != "" {
			summary.FailedCases++
			continue
		}
		summary.CompletedCases++
		summary.WarmedRows += entry.Rows
	}
	return summary
}

func summarizeByView(entries []*warmup.EntryResult) []*viewSummary {
	if len(entries) == 0 {
		return nil
	}
	index := map[string]*viewSummary{}
	for _, entry := range entries {
		if entry == nil || entry.View == "" {
			continue
		}
		current := index[entry.View]
		if current == nil {
			current = &viewSummary{View: entry.View}
			index[entry.View] = current
		}
		current.Elapsed += entry.TimeTaken
		if entry.Error != "" {
			current.FailedCases++
			continue
		}
		current.CompletedCases++
		current.WarmedRows += entry.Rows
	}
	result := make([]*viewSummary, 0, len(index))
	for _, item := range index {
		result = append(result, item)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].View < result[j].View
	})
	return result
}

func logViewSummaries(uri string, views []*view.View, result *warmup.Result) {
	if result == nil {
		return
	}
	viewsIndex := indexViewsByName(views)
	for _, summary := range summarizeByView(result.Entries) {
		fmt.Printf("[INFO] cache warmup view summary uri=%s view=%s completed_cases=%d failed_cases=%d warmed_rows=%d elapsed=%s\n",
			uri,
			summary.View,
			summary.CompletedCases,
			summary.FailedCases,
			summary.WarmedRows,
			summary.Elapsed)
		recordWarmupViewMetrics(viewsIndex[summary.View], summary)
	}
}

func joinErrors(values []string) string {
	if len(values) == 0 {
		return ""
	}
	sorted := append([]string{}, values...)
	sort.Strings(sorted)
	return strings.Join(sorted, "; ")
}

func indexViewsByName(views []*view.View) map[string]*view.View {
	if len(views) == 0 {
		return nil
	}
	result := make(map[string]*view.View, len(views))
	for _, candidate := range views {
		if candidate == nil || candidate.Name == "" {
			continue
		}
		result[candidate.Name] = candidate
	}
	return result
}

func recordWarmupViewMetrics(aView *view.View, summary *viewSummary) {
	if aView == nil || summary == nil {
		return
	}
	operation := warmupMetricOperation(aView)
	if operation == nil {
		return
	}
	end := time.Now()
	started := end.Add(-summary.Elapsed)
	runStatus := warmupRunOKKey
	if summary.FailedCases > 0 {
		runStatus = warmupRunErrorKey
	}
	operation.Begin(started)(end, runStatus)
	if summary.CompletedCases > 0 {
		operation.IncrementValueBy(warmupCasesCompletedKey, int64(summary.CompletedCases))
	}
	if summary.FailedCases > 0 {
		operation.IncrementValueBy(warmupCasesFailedKey, int64(summary.FailedCases))
	}
	if summary.WarmedRows > 0 {
		operation.IncrementValueBy(warmupRowsKey, int64(summary.WarmedRows))
	}
}

func warmupMetricOperation(aView *view.View) *gmetricx.OperationRef {
	if aView == nil {
		return nil
	}
	resource := aView.GetResource()
	if resource == nil || resource.Metrics == nil || resource.Metrics.Service == nil {
		return nil
	}
	metricName := warmupMetricName(aView)
	pkg := warmupMetricPackage(aView)
	title := aView.Name + " warmup"
	return gmetricx.NewOperationRef(resource.Metrics.Service, metricName, func() *gmetric.Operation {
		return resource.Metrics.Service.MultiOperationCounter(pkg, metricName, title, time.Millisecond, time.Minute, warmupMetricRecentBuckets, base.NewProvider(
			warmupRunOKKey,
			warmupRunErrorKey,
			warmupCasesCompletedKey,
			warmupCasesFailedKey,
			warmupRowsKey,
		))
	})
}

func warmupMetricName(aView *view.View) string {
	name := warmupMetricPackage(aView) + "." + aView.Name + ".warmup"
	return strings.ReplaceAll(name, "/", ".")
}

func warmupMetricPackage(aView *view.View) string {
	resource := aView.GetResource()
	if resource == nil {
		return warmupMetricFallbackPkg
	}
	sourceURL := url.Path(resource.SourceURL)
	parent, _ := path.Split(sourceURL)
	if idx := strings.Index(parent, "/routes/"); idx != -1 {
		return strings.Trim(parent[idx+len("/routes/"):], "/")
	}
	return warmupMetricFallbackPkg
}

func viewNames(views []*view.View) string {
	if len(views) == 0 {
		return ""
	}
	names := make([]string, 0, len(views))
	for _, candidate := range views {
		if candidate == nil {
			continue
		}
		names = append(names, candidate.Name)
	}
	return strings.Join(names, ",")
}
