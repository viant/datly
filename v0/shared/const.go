package shared

const (
	//YAMLExt yaml extension
	YAMLExt = ".yaml"
	//JSONExt json extension
	JSONExt = ".json"
)

const (
	//StatusOK response status ok
	StatusOK = "ok"
	//StatusRunning represents running status
	StatusRunning = "running"
	//StatusNoMatch response rule no matched status
	StatusNoMatch = "moMatch"
	//StatusError response error status
	StatusError = "error"
)

const ( //Binding source types
	//BindingPath binding Path source
	BindingPath = "Path"
	//BindingQueryString binding query string source
	BindingQueryString = "QueryString"
	//BindingDataView binding view view source
	BindingDataView = "DataView"
	//BindingBodyData binding view source
	BindingBodyData = "BodyData"
	//BindingDataPool binding view source
	BindingDataPool = "DataPool"
	//BindingHeader binding header source
	BindingHeader = "Header"
)

const (
	//DefaultDataOutputKey output key
	DefaultDataOutputKey = "Root"
)

//The following section defines constant for pagination template dialect (https://en.wikipedia.org/wiki/Select_(SQL)#FETCH_FIRST_clause)
const (
	//DialectSQL dialect supporting LIMIT/OFFSET keyword
	DialectSQL = "ParametrizedSQL" //regular LIMIT/OFFSET
	//DialectSQL2008 dialect ParametrizedSQL 2008
	DialectSQL2008 = "SQL2008" //FETCH
)

const (

	//ErrorTypeException unexpected error
	ErrorTypeException = "exception"
	//ErrorTypeInvalidRule invalid rule
	ErrorTypeInvalidRule = "invalidRule"
	//ErrorTypeDataValidation invalid validation
	ErrorTypeDataValidation = "invalid"
	//ErrorTypeCache cache error
	ErrorTypeCache = "cache"
	//ErrorTypeRule rule error
	ErrorTypeRule = "rule"
)

const (
	//ConfigKey config env key
	ConfigKey = "CONFIG"
)

const (
	//CardinalityOne cardinality one
	CardinalityOne = "One"
	//CardinalityMany cardinality many
	CardinalityMany = "Many"
)

const (
	//JoinTypeLeft left join type
	JoinTypeLeft = "LEFT"
	//JoinTypeInner inner join type
	JoinTypeInner = "INNER"
)

const ( //Selectors keys
	//FieldsKey fields key
	FieldsKey = "_fields"
	//OrderByKey order by key
	OrderByKey = "_orderBy"
	//CriteriaKey criteria key
	CriteriaKey = "_criteria"
	//ParamsKey optional criteria parameter key
	ParamsKey = "_params"
	//LimitKey limit key
	LimitKey = "_limit"
	//OffsetKey offset key
	OffsetKey = "_offset"
	//Metrics view only query strning
	Metrics = "_metrics"
)

const (
	MetricsAll   = "all"
	MetricsBasic = "basic"
)

const (
	//EventCreateTimeHeader
	EventCreateTimeHeader = "Event-Create-Time"
)

const (
	//EncodingGzip encoding gzip
	EncodingGzip = "gzip"
	//ContentType JSON
	ContentTypeJSON = "application/json"
	CharsetUTF8     = "charset=utf-8"
	ContentLength   = "Content-Length"
)

const (
	ColumnTypeBit     = "BIT"
	ColumnTypeBoolean = "BOOLEAN"
	ColumnTypeTinyInt = "TINYINT"

	ColumnTypeInt      = "INT"
	ColumnTypeInteger  = "INTEGER"
	ColumnTypeInt64    = "INT64"
	ColumnTypeSmallInt = "SMALLINT"
	ColumnTypeBigInt   = "BigNT"

	ColumnTypeDecimal = "DECIMAL"
	ColumnTypeFloat   = "FLOAT"
	ColumnTypeFloat64 = "FLOAT64"
	ColumnTypeNumeric = "NUMERIC"
	ColumnTypeNumber  = "NUMBER"

	ColumnTypeChar     = "CHAR"
	ColumnTypeVarchar  = "VARCHAR"
	ColumnTypeVarchar2 = "VARCHAR2"
	ColumnTypeString   = "STRING"
	ColumnTypeCBlob    = "CBLOB"
	ColumnTypeText     = "TEXT"

	ColumnTypeDate        = "DATE"
	ColumnTypeDateTime    = "DATETIME"
	ColumnTypeTimestamp   = "TIMESTAMP"
	ColumnTypeTimestampTz = "TIMESTAMPTZ"
)
