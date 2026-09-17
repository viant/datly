package validation

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/viant/govalidator"
)

const ConnectorDSNCheck = "datly_connector_dsn"

func init() {
	govalidator.Register(ConnectorDSNCheck, newConnectorDSNCheck)
}

func newConnectorDSNCheck(_ *govalidator.Field, check *govalidator.Check) (govalidator.IsValid, error) {
	if len(check.Parameters) != 2 {
		return nil, fmt.Errorf("%s expects driver and secret-ref field names", ConnectorDSNCheck)
	}
	driverField, secretField := check.Parameters[0], check.Parameters[1]
	return func(ctx context.Context, value any) (bool, error) {
		dsn := stringValue(value)
		driver, err := validationSibling(ctx, driverField)
		if err != nil {
			return false, err
		}
		secret, err := validationSibling(ctx, secretField)
		if err != nil {
			return false, err
		}
		if strings.TrimSpace(dsn) == "" {
			return strings.TrimSpace(secret) != "", nil
		}
		return validConnectorDSN(strings.ToLower(strings.TrimSpace(driver)), strings.TrimSpace(dsn)), nil
	}, nil
}

func validationSibling(ctx context.Context, name string) (string, error) {
	session, ok := ctx.Value(govalidator.SessionKey).(*govalidator.Session)
	if !ok || session == nil || session.ParentValue == nil {
		return "", fmt.Errorf("%s validation session is unavailable", ConnectorDSNCheck)
	}
	value := reflect.ValueOf(session.ParentValue)
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return "", nil
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return "", fmt.Errorf("%s validation parent is not a struct", ConnectorDSNCheck)
	}
	field := value.FieldByName(name)
	if !field.IsValid() || !field.CanInterface() {
		return "", fmt.Errorf("%s validation field %q is unavailable", ConnectorDSNCheck, name)
	}
	return stringValue(field.Interface()), nil
}

func stringValue(value any) string {
	if value == nil {
		return ""
	}
	actual := reflect.ValueOf(value)
	for actual.Kind() == reflect.Pointer {
		if actual.IsNil() {
			return ""
		}
		actual = actual.Elem()
	}
	if actual.Kind() != reflect.String {
		return ""
	}
	return actual.String()
}

func validConnectorDSN(driver, dsn string) bool {
	switch driver {
	case "mysql":
		_, err := mysql.ParseDSN(dsn)
		return err == nil
	case "viant/bigquery":
		return validURLDSN(dsn, "bigquery", true, true)
	case "pg":
		if validURLDSN(dsn, "postgres", true, true) || validURLDSN(dsn, "postgresql", true, true) {
			return true
		}
		return validPostgresKeywordDSN(dsn)
	case "sqlite":
		if dsn == ":memory:" {
			return true
		}
		parsed, err := url.Parse(dsn)
		return err == nil && parsed.Scheme == "file" && (parsed.Opaque != "" || parsed.Path != "")
	case "viant/aerospike":
		return validURLDSN(dsn, "aerospike", true, true)
	default:
		return false
	}
}

func validURLDSN(dsn, scheme string, requireHost, requirePath bool) bool {
	parsed, err := url.Parse(dsn)
	if err != nil || !strings.EqualFold(parsed.Scheme, scheme) {
		return false
	}
	if requireHost && strings.TrimSpace(parsed.Host) == "" {
		return false
	}
	return !requirePath || strings.Trim(parsed.Path, "/") != ""
}

func validPostgresKeywordDSN(dsn string) bool {
	values := map[string]string{}
	for _, token := range strings.Fields(dsn) {
		key, value, ok := strings.Cut(token, "=")
		if !ok || strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
			return false
		}
		values[strings.ToLower(strings.TrimSpace(key))] = strings.TrimSpace(value)
	}
	return values["host"] != "" && (values["dbname"] != "" || values["database"] != "")
}
