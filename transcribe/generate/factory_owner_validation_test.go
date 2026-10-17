package generate

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFactoryOwnerClosureRevalidatedInForestProjection(t *testing.T) {
	fixture := &snapshotFixture{}
	fixture.init(t)
	p := fixture.persistence()
	p.files = nil
	p.userFiles = nil
	ownerPath := filepath.Join(fixture.dir, "hooks.go")
	expected, err := os.ReadFile(ownerPath)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	p.plan = &Plan{ExternalHandler: &ExternalHandler{ValidateOwnerSources: func(mapPath func(string) string) error {
		calls++
		actual, err := os.ReadFile(mapPath(ownerPath))
		if err != nil {
			return err
		}
		if string(actual) != string(expected) {
			return fmt.Errorf("owner closure drift")
		}
		return nil
	}}}
	forests, _, _, err := prepareScaffoldForests([]*scaffoldPersistence{p})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupScaffoldForests(forests)
	if len(forests) != 1 || calls == 0 {
		t.Fatal("owner closure not validated before staging")
	}
	if err := os.WriteFile(filepath.Join(forests[0].stage, "hooks.go"), []byte("staged drift"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := publishScaffoldForests(forests); err == nil || !strings.Contains(err.Error(), "owner closure drift") {
		t.Fatalf("publication error=%v", err)
	}
	actual, err := os.ReadFile(ownerPath)
	if err != nil || string(actual) != string(expected) {
		t.Fatal("failed owner proof modified original package")
	}
	if calls < 2 {
		t.Fatal("forest publication omitted owner revalidation")
	}
}

func TestFactoryOwnerClosureUsesPublishedForestAndUnpublishedStage(t *testing.T) {
	owner, consumer := &snapshotFixture{}, &snapshotFixture{}
	owner.init(t)
	consumer.init(t)
	ownerPath, consumerPath := filepath.Join(owner.dir, "hooks.go"), filepath.Join(consumer.dir, "hooks.go")
	expectedOwner, err := os.ReadFile(ownerPath)
	if err != nil {
		t.Fatal(err)
	}
	expectedConsumer, err := os.ReadFile(consumerPath)
	if err != nil {
		t.Fatal(err)
	}
	var forests []*scaffoldForest
	unpublishedSeen, publishedSeen := false, false
	ownerPlan, consumerPlan := owner.persistence(), consumer.persistence()
	ownerPlan.files = nil
	ownerPlan.userFiles = nil
	consumerPlan.files = nil
	consumerPlan.userFiles = nil
	ownerPlan.plan = &Plan{ExternalHandler: &ExternalHandler{ValidateOwnerSources: func(mapPath func(string) string) error {
		mapped := mapPath(consumerPath)
		if len(forests) == 2 {
			if forests[1].published || mapped != filepath.Join(forests[1].stage, "hooks.go") {
				return fmt.Errorf("unpublished consumer must use staged source")
			}
			unpublishedSeen = true
		}
		content, err := os.ReadFile(mapped)
		if err != nil {
			return err
		}
		if string(content) != string(expectedConsumer) {
			return fmt.Errorf("consumer closure drift")
		}
		return nil
	}}}
	consumerPlan.plan = &Plan{ExternalHandler: &ExternalHandler{ValidateOwnerSources: func(mapPath func(string) string) error {
		mapped := mapPath(ownerPath)
		if len(forests) == 2 {
			if !forests[0].published || mapped != ownerPath {
				return fmt.Errorf("published owner must use live source")
			}
			publishedSeen = true
		}
		content, err := os.ReadFile(mapped)
		if err != nil {
			return err
		}
		if string(content) != string(expectedOwner) {
			return fmt.Errorf("owner closure drift")
		}
		return nil
	}}}
	forests, _, _, err = prepareScaffoldForests([]*scaffoldPersistence{ownerPlan, consumerPlan})
	if err != nil {
		t.Fatal(err)
	}
	if len(forests) != 2 {
		t.Fatalf("forests=%d", len(forests))
	}
	if err := publishScaffoldForests(forests); err != nil {
		t.Fatal(err)
	}
	if !unpublishedSeen || !publishedSeen {
		t.Fatal("publication omitted staged or published owner validation")
	}
	for _, f := range forests {
		if !f.published {
			t.Fatal("forest unpublished")
		}
		if _, err := os.Stat(f.stage); !os.IsNotExist(err) {
			t.Fatalf("published stage still exists: %v", err)
		}
	}
}
