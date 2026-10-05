package jobs

import (
	"fmt"
	"net/url"
)

// routeURI is the selected route instance, not the query used to execute it.
// Query values remain in durable URI/state for replay and the reader's SQL scope
// guards. In particular, sync controls must not create another durable job.
func routeURI(raw string) (string, error) {
	uri, err := url.ParseRequestURI(raw)
	if err != nil {
		return "", fmt.Errorf("invalid job route URI")
	}
	uri.RawQuery = ""
	uri.ForceQuery = false
	return uri.String(), nil
}
