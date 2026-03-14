package debugruntime

import (
	"log"
	"os"
	"sync"

	"github.com/google/gops/agent"
)

var gopsOnce sync.Once

func StartGopsFromEnv() {
	if os.Getenv("DATLY_ENABLE_GOPS") == "" {
		return
	}
	gopsOnce.Do(func() {
		opts := agent.Options{}
		if addr := os.Getenv("DATLY_GOPS_ADDR"); addr != "" {
			opts.Addr = addr
		}
		if err := agent.Listen(opts); err != nil {
			log.Printf("failed to start gops agent: %v", err)
		}
	})
}
