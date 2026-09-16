package repository

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/shape/typectx"
	"github.com/viant/datly/view"
)

func TestUnmarshalComponentMap_PropagatesTopLevelTypeContext(t *testing.T) {
	model := map[string]any{
		"TypeContext": map[string]any{
			"DefaultPackage": "mdp/performance",
			"Imports": []any{
				map[string]any{
					"Alias":   "perf",
					"Package": "github.com/acme/mdp/performance",
				},
			},
		},
		"Components": []any{
			map[string]any{
				"URI":    "/v1/api/sample",
				"Method": "GET",
				"View": map[string]any{
					"Ref": "sample",
				},
			},
		},
		"Resource": map[string]any{
			"Views": []any{
				map[string]any{"Name": "sample"},
			},
		},
	}
	components, err := unmarshalComponentMap(model, true)
	require.NoError(t, err)
	require.Len(t, components.Components, 1)
	require.NotNil(t, components.Components[0].TypeContext)
	require.Equal(t, "mdp/performance", components.Components[0].TypeContext.DefaultPackage)
	require.Len(t, components.Components[0].TypeContext.Imports, 1)
	require.Equal(t, "perf", components.Components[0].TypeContext.Imports[0].Alias)
}

func TestUnmarshalComponentMap_PerComponentTypeContextOverridesTopLevel(t *testing.T) {
	model := map[string]any{
		"TypeContext": map[string]any{
			"DefaultPackage": "top/level",
		},
		"Components": []any{
			map[string]any{
				"URI":    "/v1/api/sample",
				"Method": "GET",
				"View": map[string]any{
					"Ref": "sample",
				},
				"TypeContext": map[string]any{
					"DefaultPackage": "component/level",
					"Imports": []any{
						map[string]any{
							"Alias":   "foo",
							"Package": "github.com/acme/foo",
						},
					},
				},
			},
		},
		"Resource": map[string]any{
			"Views": []any{
				map[string]any{"Name": "sample"},
			},
		},
	}
	components, err := unmarshalComponentMap(model, true)
	require.NoError(t, err)
	require.Len(t, components.Components, 1)
	require.NotNil(t, components.Components[0].TypeContext)
	require.Equal(t, "component/level", components.Components[0].TypeContext.DefaultPackage)
	require.Len(t, components.Components[0].TypeContext.Imports, 1)
	require.Equal(t, "foo", components.Components[0].TypeContext.Imports[0].Alias)
}

func TestUnmarshalComponentMap_NoTypeContextRemainsNil(t *testing.T) {
	model := map[string]any{
		"Components": []any{
			map[string]any{
				"URI":    "/v1/api/sample",
				"Method": "GET",
				"View": map[string]any{
					"Ref": "sample",
				},
			},
		},
		"Resource": map[string]any{
			"Views": []any{
				map[string]any{"Name": "sample"},
			},
		},
	}
	components, err := unmarshalComponentMap(model, true)
	require.NoError(t, err)
	require.Len(t, components.Components, 1)
	require.Nil(t, components.Components[0].TypeContext)
}

func TestUnmarshalComponentMap_TopLevelTypeContext_DisabledByFlag(t *testing.T) {
	model := map[string]any{
		"TypeContext": map[string]any{
			"DefaultPackage": "mdp/performance",
		},
		"Components": []any{
			map[string]any{
				"URI":    "/v1/api/sample",
				"Method": "GET",
				"View": map[string]any{
					"Ref": "sample",
				},
			},
		},
		"Resource": map[string]any{
			"Views": []any{
				map[string]any{"Name": "sample"},
			},
		},
	}
	components, err := unmarshalComponentMap(model, false)
	require.NoError(t, err)
	require.Len(t, components.Components, 1)
	require.Nil(t, components.Components[0].TypeContext)
}

func TestResolveComponentTypeContext_FromTemplateSource(t *testing.T) {
	component := &Component{
		View: &view.View{
			Template: view.NewTemplate(`
#set($_ = $package('mdp/performance'))
#set($_ = $import('perf', 'github.com/acme/mdp/performance'))
SELECT ID FROM REPORT r`),
		},
	}
	resolved := resolveComponentTypeContext(component)
	require.NotNil(t, resolved)
	require.Equal(t, "mdp/performance", resolved.DefaultPackage)
	require.Len(t, resolved.Imports, 1)
	require.Equal(t, "perf", resolved.Imports[0].Alias)
}

func TestResolveComponentTypeContext_PrefersExisting(t *testing.T) {
	component := &Component{
		TypeContext: &typectx.Context{
			DefaultPackage: " custom/pkg ",
			Imports: []typectx.Import{
				{Alias: " a ", Package: " github.com/acme/a "},
			},
		},
	}
	resolved := resolveComponentTypeContext(component)
	require.NotNil(t, resolved)
	require.Equal(t, "custom/pkg", resolved.DefaultPackage)
	require.Len(t, resolved.Imports, 1)
	require.Equal(t, "a", resolved.Imports[0].Alias)
	require.Equal(t, "github.com/acme/a", resolved.Imports[0].Package)
}
