package reader

//Options represents generic reader options
type Options []interface{}

//Option represent a reader option
type Option interface{}

//AllowUnmapped represents Service option. If false, will cause an error if reader tries to read column that is not present in struct.
type AllowUnmapped bool
