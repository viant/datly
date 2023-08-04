package config

import (
	"net/http"
)

type (
	BeforeFetcher interface {
		BeforeFetch(response http.ResponseWriter, request *http.Request) error
	}

	AfterFetcher interface {
		AfterFetch(dest interface{}, response http.ResponseWriter, req *http.Request) error
	}

	ClosableAfterFetcher interface {
		AfterFetch(dest interface{}, response http.ResponseWriter, req *http.Request) (responseClosed bool, err error)
	}

	ClosableBeforeFetcher interface {
		BeforeFetch(response http.ResponseWriter, request *http.Request) (responseClosed bool, err error)
	}
)
