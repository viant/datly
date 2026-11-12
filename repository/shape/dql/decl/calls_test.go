package decl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScanCalls_DollarStrict(t *testing.T) {
	input := "$connector('dev') $dest('a.go')"
	calls, errs := ScanCalls(input, CallScanOptions{
		AllowedNames:  map[string]bool{"connector": true, "dest": true},
		RequireDollar: true,
		AllowDollar:   true,
		Strict:        true,
	})
	require.Empty(t, errs)
	require.Len(t, calls, 2)
	assert.Equal(t, "connector", calls[0].Name)
	assert.Equal(t, []string{"'dev'"}, calls[0].Args)
	assert.True(t, calls[0].Dollar)
	assert.Equal(t, "dest", calls[1].Name)
}

func TestScanCalls_ReportsMalformedCallOffset(t *testing.T) {
	input := "$dest('a.go'"
	calls, errs := ScanCalls(input, CallScanOptions{
		AllowedNames:  map[string]bool{"dest": true},
		RequireDollar: true,
		AllowDollar:   true,
		Strict:        true,
	})
	require.Empty(t, calls)
	require.Len(t, errs, 1)
	assert.Equal(t, "dest", errs[0].Name)
	assert.Equal(t, 0, errs[0].Offset)
}

func TestScanCalls_BareOnly(t *testing.T) {
	input := "dest(vendor,'vendor.go'), type(vendor,'Vendor'), $dest('x.go')"
	calls, errs := ScanCalls(input, CallScanOptions{
		AllowedNames:  map[string]bool{"dest": true, "type": true},
		RequireDollar: false,
		AllowDollar:   false,
		Strict:        false,
	})
	require.Empty(t, errs)
	require.Len(t, calls, 2)
	assert.Equal(t, "dest", calls[0].Name)
	assert.Equal(t, "type", calls[1].Name)
}
