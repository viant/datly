package golang

import (
	"strings"
	"testing"
)

func TestQueueContractCannotDisappearInLegacyDirectLowering(t *testing.T) {
	_, semantic, _ := recursivePatchFixture(t)
	if _, err := Lower(semantic, recursiveGoConfig(semantic, nil)); err != nil {
		t.Fatal("default legacy behavior", err)
	}
	for _, contract := range []string{"source-row", "source-slice", "unknown"} {
		semantic.Root.Write.QueueContract = contract
		if _, err := Lower(semantic, recursiveGoConfig(semantic, nil)); err == nil || !strings.Contains(err.Error(), "queue_contract is unavailable") {
			t.Fatal(contract, err)
		}
	}
	// A child contract must fail as explicitly as the root role.
	semantic.Root.Write.QueueContract = ""
	semantic.Root.Relations[0].Child.Write.QueueContract = "source-row"
	if _, err := Lower(semantic, recursiveGoConfig(semantic, nil)); err == nil {
		t.Fatal("child contract silently dropped")
	}
}
