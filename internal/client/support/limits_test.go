package support

import (
	"context"
	"testing"
	"time"

	"github.com/viant/xdatly/client"
)

func TestZeroLimitsAreUnlimited(t *testing.T) {
	for _, value := range []client.Limits{{}, {Timeout: "0"}, {Timeout: "0s"}} {
		timeout, size, err := Limits(value)
		if err != nil || timeout != 0 || size != 0 {
			t.Fatalf("limits=%+v timeout=%v size=%d err=%v", value, timeout, size, err)
		}
		ctx, cancel := WithTimeout(context.Background(), timeout)
		if _, exists := ctx.Deadline(); exists {
			t.Fatal("zero timeout imposed a deadline")
		}
		cancel()
		if ctx.Err() != context.Canceled {
			t.Fatal("caller cannot cancel unlimited operation")
		}
	}
	timeout, size, err := Limits(client.Limits{Timeout: "10m", MaxResponseBytes: 1 << 40})
	if err != nil || timeout != 10*time.Minute || size != 1<<40 {
		t.Fatalf("explicit positive limits altered: %v %d %v", timeout, size, err)
	}
	parent, stop := context.WithCancel(context.Background())
	ctx, cancel := WithTimeout(parent, 0)
	defer cancel()
	stop()
	if ctx.Err() != context.Canceled {
		t.Fatal("unlimited timeout detached caller cancellation")
	}
}
