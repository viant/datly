package locator

// Unmarshal converts data into dest
type Unmarshal func([]byte, interface{}) error
