package reader

import "time"

var Now = time.Now
var Dif = func(t1, t2 time.Time) time.Duration {
	return t1.Sub(t2)
}
