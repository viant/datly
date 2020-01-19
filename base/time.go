package base

import (
	"time"
)

//ElapsedInMs return elapsed duration in ms
func ElapsedInMs(started time.Time) int {
	elapsed :=  time.Now().Sub(started)
	return int(elapsed/1000000)
}
