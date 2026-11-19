package shape

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type captureCompiler struct {
	last CompileOptions
}

func (c *captureCompiler) Compile(_ context.Context, source *Source, opts ...CompileOption) (*PlanResult, error) {
	compiled := &CompileOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(compiled)
		}
	}
	c.last = *compiled
	return &PlanResult{Source: source}, nil
}

func TestEngine_Compile_UsesLegacyParityDefaults(t *testing.T) {
	compiler := &captureCompiler{}
	engine := New(WithCompiler(compiler))

	_, err := engine.compile(context.Background(), &Source{Name: "orders", DQL: "SELECT 1"})
	require.NoError(t, err)
	assert.False(t, compiler.last.Strict)
	assert.Equal(t, CompileProfileCompat, compiler.last.Profile)
	assert.Equal(t, CompileMixedModeExecWins, compiler.last.MixedMode)
	assert.Equal(t, CompileUnknownNonReadWarn, compiler.last.UnknownNonReadMode)
	assert.Equal(t, CompileColumnDiscoveryAuto, compiler.last.ColumnDiscoveryMode)
}

func TestEngine_Compile_ForwardsCustomDefaults(t *testing.T) {
	compiler := &captureCompiler{}
	engine := New(
		WithCompiler(compiler),
		WithStrict(true),
		WithCompileProfileDefault(CompileProfileStrict),
		WithMixedModeDefault(CompileMixedModeReadWins),
		WithUnknownNonReadModeDefault(CompileUnknownNonReadError),
		WithColumnDiscoveryModeDefault(CompileColumnDiscoveryOff),
	)

	_, err := engine.compile(context.Background(), &Source{Name: "orders", DQL: "SELECT 1"})
	require.NoError(t, err)
	assert.True(t, compiler.last.Strict)
	assert.Equal(t, CompileProfileStrict, compiler.last.Profile)
	assert.Equal(t, CompileMixedModeReadWins, compiler.last.MixedMode)
	assert.Equal(t, CompileUnknownNonReadError, compiler.last.UnknownNonReadMode)
	assert.Equal(t, CompileColumnDiscoveryOff, compiler.last.ColumnDiscoveryMode)
}

func TestEngine_Compile_LegacyDefaultsOption(t *testing.T) {
	compiler := &captureCompiler{}
	engine := New(
		WithCompiler(compiler),
		WithStrict(true),
		WithCompileProfileDefault(CompileProfileStrict),
		WithMixedModeDefault(CompileMixedModeReadWins),
		WithUnknownNonReadModeDefault(CompileUnknownNonReadError),
		WithLegacyTranslatorDefaults(),
	)

	_, err := engine.compile(context.Background(), &Source{Name: "orders", DQL: "SELECT 1"})
	require.NoError(t, err)
	assert.False(t, compiler.last.Strict)
	assert.Equal(t, CompileProfileCompat, compiler.last.Profile)
	assert.Equal(t, CompileMixedModeExecWins, compiler.last.MixedMode)
	assert.Equal(t, CompileUnknownNonReadWarn, compiler.last.UnknownNonReadMode)
	assert.Equal(t, CompileColumnDiscoveryAuto, compiler.last.ColumnDiscoveryMode)
}
