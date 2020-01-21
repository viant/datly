package base

const (
	//YAMLExt yaml extension
	YAMLExt = ".yaml"
	//JSONExt json extension
	JSONExt = ".json"
)

const (
	//StatusOK response status ok
	StatusOK = "ok"
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
	//BindingDataView binding data view source
	BindingDataView = "DataView"
	//BindingData binding data source
	BindingData = "Data"
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
	DialectSQL = "SQL" //regular LIMIT/OFFSET
	//DialectSQL2008 dialect SQL 2008
	DialectSQL2008 = "SQL2008" //FETCH
)

const (

	//ErrorTypeException unexpected error
	ErrorTypeException = "exception"
	//ErrorTypeInvalidRule invalid rule
	ErrorTypeInvalidRule = "invalidRule"
	//ErrorTypeDataValidation invalid validation
	ErrorTypeDataValidation = "invalid"
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

const ( //Selector keys
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
)
