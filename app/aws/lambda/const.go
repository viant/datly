package lambda

import "time"

const (
	CompressionLimit  = (2 * 1024 * 1024)
	BodyLimit         = (6 * 1024 * 1024) - 128*1024
	ResponseFolder    = "response"
	PreSignTimeToLive = 5 * time.Minute
	APIGWTimeout      = 28 * time.Second
	locationHeader    = "Location"
)
