package validate

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

func TestValidateRelations_AllowsAliasSourceAndNamespace(t *testing.T) {
	parent := &view.View{
		Name: "vendor",
		Columns: view.Columns{
			view.NewColumn("ID", "int", nil, false),
		},
	}
	child := &view.View{
		Name: "products",
		Columns: view.Columns{
			view.NewColumn("VendorID", "int", nil, false, view.WithColumnTag(`source:"VENDOR_ID"`)),
		},
	}
	parent.With = []*view.Relation{{
		Name:        "products",
		Cardinality: state.Many,
		Holder:      "Products",
		On:          view.Links{&view.Link{Column: "vendor.ID"}},
		Of: &view.ReferenceView{
			View: *child,
			On:   view.Links{&view.Link{Column: "VENDOR_ID"}},
		},
	}}
	resource := view.EmptyResource()
	resource.Views = append(resource.Views, parent, child)
	require.NoError(t, ValidateRelations(resource, parent))
}

func TestValidateRelations_DetailedMissingError(t *testing.T) {
	parent := &view.View{
		Name: "vendor",
		Columns: view.Columns{
			view.NewColumn("ID", "int", nil, false),
		},
	}
	child := &view.View{
		Name: "products",
		Columns: view.Columns{
			view.NewColumn("VendorID", "int", nil, false),
		},
	}
	parent.With = []*view.Relation{{
		Name:        "products",
		Cardinality: state.Many,
		Holder:      "Products",
		On:          view.Links{&view.Link{Column: "MISSING_PARENT"}},
		Of: &view.ReferenceView{
			View: *child,
			On:   view.Links{&view.Link{Column: "MISSING_CHILD"}},
		},
	}}
	resource := view.EmptyResource()
	resource.Views = append(resource.Views, parent, child)
	err := ValidateRelations(resource, parent)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing parent column \"MISSING_PARENT\"")
	require.Contains(t, err.Error(), "missing ref column \"MISSING_CHILD\"")
	require.Contains(t, err.Error(), "parent=\"vendor\"")
	require.Contains(t, err.Error(), "ref=\"products\"")
}
