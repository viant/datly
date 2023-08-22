package locator

// Unmarshal converts data into dest
type Unmarshal func(data []byte, dest any) error
