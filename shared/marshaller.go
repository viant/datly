package shared

// Unmarshal converts data to destination, destination has to be a pointer to desired output type
type Unmarshal func(data []byte, destination interface{}) error
