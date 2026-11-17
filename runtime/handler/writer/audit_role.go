package writer

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	sqlxio "github.com/viant/sqlx/io"
)

// Audit is a persistence role, independent of logging/diff visibility. The
// field accompanies a business change and cannot be an identity or control.
func validateAuditField(field reflect.StructField, column *spec.Column) error {
	name := strings.TrimSpace(strings.Split(field.Tag.Get("sqlx"), ",")[0])
	if sqlxio.ParseTag(field.Tag).Transient || name == "" && column == nil {
		return fmt.Errorf("writer audit field %s must be persisted", field.Name)
	}
	if typecatalog.SQLXPrimaryKey(field.Tag, column != nil && column.PrimaryKey) ||
		column != nil && (column.DeleteMarker || column.ConcurrencyToken) {
		return fmt.Errorf("writer audit field %s cannot be an identity, deletion or concurrency control", field.Name)
	}
	if field.Tag.Get("invariant") != "" {
		return fmt.Errorf("writer audit field %s cannot activate a business invariant", field.Name)
	}
	return nil
}
