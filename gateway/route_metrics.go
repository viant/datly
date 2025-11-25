package gateway

import (
	"context"
	"path"
	"strings"
	"time"

	dlogger "github.com/viant/datly/logger"
	"github.com/viant/datly/repository"
	gprovider "github.com/viant/gmetric/provider"
)

// ensureRouteCounter pre-registers a per-route counter and returns a logger-compatible adapter.
func (r *Router) ensureRouteCounter(ctx context.Context, prov *repository.Provider) dlogger.Counter {
	if r.metrics == nil || prov == nil {
		return nil
	}
	component, err := prov.Component(ctx)
	if err != nil || component == nil || component.View == nil {
		return nil
	}

	v := component.View

	// Derive a stable package from resource URL similar to view.discoverPackage
	pkg := "datly"
	if res := v.GetResource(); res != nil {
		src := res.SourceURL
		// Extract the dir and find the segment after "/routes/"
		parent, _ := path.Split(src)
		if idx := strings.Index(parent, "/routes/"); idx != -1 {
			pkg = strings.Trim(parent[idx+len("/routes/"):], "/")
		}
	}

	// Build a metric operation name aligned with view metrics namespace, but scoped to component URI (.request)
	method := component.Path.Method
	normURI := normalizeURI(component.URI)
	name := strings.Trim(normURI, "/") + ".request"
	name = strings.ReplaceAll(name, "/", ".")
	metricName := pkg + "." + name
	if method != "" && !strings.EqualFold(method, "GET") {
		metricName = method + ":" + metricName
	}
	metricName = strings.ReplaceAll(metricName, "/", ".")

	cnt := r.metrics.LookupOperation(metricName)
	if cnt == nil {
		// Title: human-friendly
		title := v.Name + " request"
		cnt = r.metrics.MultiOperationCounter(pkg, metricName, title, time.Millisecond, time.Minute, 2, gprovider.NewBasic())
	}
	return dlogger.NewCounter(cnt)
}

// normalizeURI replaces path parameters like {id} with a constant token to limit cardinality.
func normalizeURI(uri string) string {
	res := uri
	for {
		i := strings.Index(res, "{")
		if i == -1 {
			break
		}
		j := strings.Index(res[i:], "}")
		if j == -1 {
			break
		}
		j = i + j + 1
		res = res[:i] + "T" + res[j:]
	}
	return res
}
