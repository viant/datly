package httputils

const (
	AllowOriginHeader  = "Access-Control-Allow-Origin"
	AllowHeadersHeader = "Access-Control-Allow-Headers"
	AllowMethodsHeader = "Access-Control-Allow-Methods"

	AllControlRequestHeader = "Access-Control-Request-Method"

	AllowCredentialsHeader = "Access-Control-Allow-Credentials"
	ExposeHeadersHeader    = "Access-Control-Expose-Headers"
	MaxAgeHeader           = "Access-Control-Max-Age"

	DatlyRequestMetricsHeader      = "Datly-Show-Metrics"
	DatlyInfoHeaderValue           = "info"
	DatlyDebugHeaderValue          = "debug"
	DatlyRequestDisableCacheHeader = "Datly-Disable-Cache"
	DatlyResponseHeaderMetrics     = "Datly-Metrics"

	DatlyServiceTimeHeader = "Datly-Service-Time"
	DatlyServiceInitHeader = "Datly-Service-init"

	//ContentTypeJSON json content type
	ContentTypeJSON = "application/json"

	CharsetUTF8 = "charset=utf-8"
	//EncodingGzip encoding gzip
	EncodingGzip = "gzip"

	ContentLength = "Content-Length"
)
