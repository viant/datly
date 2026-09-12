package decl

// Kind identifies parsed declaration function.
type Kind string

const (
	KindCast                  Kind = "cast"
	KindTag                   Kind = "tag"
	KindSetLimit              Kind = "set_limit"
	KindAllowNulls            Kind = "allow_nulls"
	KindSetPartitioner        Kind = "set_partitioner"
	KindUseConnector          Kind = "use_connector"
	KindMatchStrategy         Kind = "match_strategy"
	KindCompressAboveSize     Kind = "compress_above_size"
	KindBatchSize             Kind = "batch_size"
	KindRelationalConcurrency Kind = "relational_concurrency"
	KindPublishParent         Kind = "publish_parent"
	KindCardinality           Kind = "cardinality"
	KindPackage               Kind = "package"
	KindImport                Kind = "import"
)

// Declaration represents one parsed function declaration in DQL.
type Declaration struct {
	Kind   Kind
	Raw    string
	Offset int
	Args   []string

	// Normalized fields for known declarations.
	Target    string // first argument (alias/column)
	DataType  string // cast(... as type)
	Tag       string // tag(..., "...") payload
	Limit     string // set_limit(..., N)
	Connector string // use_connector(view, connector)
	Strategy  string // match_strategy(view, strategy)
	Partition string // set_partitioner(view, partitioner, concurrency)
	Size      string // compress_above_size(size)
	Value     string // generic second argument (batch_size, relational_concurrency, cardinality)
	Package   string // package(default/package)
	Alias     string // import(alias, package/path)
}
