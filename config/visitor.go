package config

import "net/http"

type (
	Visitor struct {
		name           string
		_afterFetcher  AfterFetcherFn
		_beforeFetcher BeforeFetcherFn
	}

	BeforeFetcherFn func(response http.ResponseWriter, request *http.Request) error
	AfterFetcherFn  func(dest interface{}, response http.ResponseWriter, req *http.Request) error
)

func (v *Visitor) BeforeFetch(response http.ResponseWriter, request *http.Request) error {
	if v._beforeFetcher == nil {
		return nil
	}

	return v._beforeFetcher(response, request)
}

func (v *Visitor) AfterFetch(dest interface{}, response http.ResponseWriter, req *http.Request) error {
	if v._afterFetcher == nil {
		return nil
	}

	return v._afterFetcher(dest, response, req)
}

func NewVisitor(name string, visitor interface{}) *Visitor {
	beforeFetcherFn, _ := asBeforeFetcher(visitor)
	afterFetcherFn, _ := asAfterFetcher(visitor)

	return &Visitor{
		name:           name,
		_beforeFetcher: beforeFetcherFn,
		_afterFetcher:  afterFetcherFn,
	}
}

func asAfterFetcher(visitor interface{}) (AfterFetcherFn, bool) {
	afterFetcher, ok := visitor.(AfterFetcher)
	if ok {
		return afterFetcher.AfterFetch, true
	}

	afterFetcherClosable, ok := visitor.(ClosableAfterFetcher)
	if ok {
		return func(dest interface{}, response http.ResponseWriter, req *http.Request) error {
			_, err := afterFetcherClosable.AfterFetch(dest, response, req)
			return err
		}, true
	}

	return nil, false
}

func asBeforeFetcher(visitor interface{}) (BeforeFetcherFn, bool) {
	fetcher, ok := visitor.(BeforeFetcher)
	if ok {
		return fetcher.BeforeFetch, true
	}

	closableFetcher, ok := visitor.(ClosableBeforeFetcher)
	if !ok {
		return nil, false
	}

	return func(response http.ResponseWriter, request *http.Request) error {
		_, err := closableFetcher.BeforeFetch(response, request)
		return err
	}, ok
}

func (v *Visitor) Name() string {
	return v.name
}
