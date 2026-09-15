package index

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe"
)

func benchmarkFixture(b *testing.B, components int) (Config, string) {
	b.Helper()
	root := b.TempDir()
	(testharness.GeneratedModule{Path: "example.com/bench"}).Write(b, root)
	for index := 0; index < components; index++ {
		name := fmt.Sprintf("Component%04d", index)
		path := fmt.Sprintf("/components/%04d/{id}", index)
		writeFixture(b, root, filepath.ToSlash(filepath.Join("services", fmt.Sprintf("p%04d", index), "holder.go")), holderSource(name, "GET", path, ""))
	}
	return Config{BaseDir: root, Include: []string{"example.com/bench/services/..."}}, root
}

func benchmarkColdBootstrap(b *testing.B, components int) {
	config, _ := benchmarkFixture(b, components)
	b.ReportMetric(float64(components), "components")
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := (Builder{Config: config}).Build(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkColdBootstrap100(b *testing.B)  { benchmarkColdBootstrap(b, 100) }
func BenchmarkColdBootstrap1000(b *testing.B) { benchmarkColdBootstrap(b, 1000) }

func BenchmarkEagerDiscovery100(b *testing.B) {
	benchmarkEagerDiscovery(b, 100)
}

func BenchmarkEagerDiscovery1000(b *testing.B) { benchmarkEagerDiscovery(b, 1000) }

func benchmarkEagerDiscovery(b *testing.B, components int) {
	config, _ := benchmarkFixture(b, components)
	b.ReportMetric(float64(components), "components")
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		project, err := (&transcribe.Discovery{BaseDir: config.BaseDir, Include: config.Include}).Compile(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		if len(project.Components) != components {
			b.Fatalf("components = %d", len(project.Components))
		}
	}
}

func BenchmarkGenerationFirstLoadOverhead1000(b *testing.B) {
	config, _ := benchmarkFixture(b, 1000)
	snapshot, err := (Builder{Config: config}).Build(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	entry := snapshot.entries[0]
	materializer := MaterializeFunc(func(_ context.Context, entry *Entry, _ Resolver) (*Loaded, error) {
		return loadedEntry(entry, nil), nil
	})
	b.ReportMetric(1000, "indexed_components")
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		generation := newGeneration(uint64(index+1), snapshot, materializer)
		if _, err := generation.Load(context.Background(), entry.Key()); err != nil {
			b.Fatal(err)
		}
		generation.retire()
	}
}

func BenchmarkRealFirstComponentCompile1000(b *testing.B) {
	config, _ := benchmarkFixture(b, 1000)
	snapshot, err := (Builder{Config: config}).Build(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	entry := snapshot.entries[0]
	b.ReportMetric(1000, "indexed_components")
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		project, compileErr := (&transcribe.Discovery{BaseDir: config.BaseDir, Include: []string{entry.Key().Scope}}).Compile(context.Background())
		if compileErr != nil {
			b.Fatal(compileErr)
		}
		var component = project.Components[0].Component
		if _, compileErr = bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[struct{}]()}); compileErr != nil {
			b.Fatal(compileErr)
		}
	}
}

func BenchmarkCachedComponentHit1000(b *testing.B) {
	config, _ := benchmarkFixture(b, 1000)
	snapshot, err := (Builder{Config: config}).Build(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	entry := snapshot.entries[0]
	generation := newGeneration(1, snapshot, MaterializeFunc(func(_ context.Context, entry *Entry, _ Resolver) (*Loaded, error) {
		return loadedEntry(entry, nil), nil
	}))
	if _, err := generation.Load(context.Background(), entry.Key()); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(1000, "indexed_components")
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := generation.Load(context.Background(), entry.Key()); err != nil {
			b.Fatal(err)
		}
	}
	generation.retire()
}
