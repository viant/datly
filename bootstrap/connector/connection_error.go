package connector

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"syscall"
)

// connectionError reports structured diagnostics only. Driver messages may
// contain credentials or DSNs, so never wrap or print the original error.
func connectionError(name, driver string, err error) error {
	detail := "database rejected the connection"
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		detail = "connection timed out"
	case errors.Is(err, context.Canceled):
		detail = "connection canceled"
	case errors.Is(err, syscall.ECONNREFUSED):
		detail = "connection refused"
	default:
		var dns *net.DNSError
		var network net.Error
		if errors.As(err, &dns) {
			detail = "DNS lookup failed"
		} else if errors.As(err, &network) && network.Timeout() {
			detail = "connection timed out"
		} else if code, state, ok := mysqlDiagnostic(err); ok {
			detail = fmt.Sprintf("MySQL error %d (SQLSTATE %s)", code, state)
			switch code {
			case 1044:
				detail += ": database access denied"
			case 1045:
				detail += ": authentication rejected"
			case 1049:
				detail += ": database does not exist"
			case 1064, 1231:
				detail += ": invalid connection session settings"
			case 1130:
				detail += ": client host is not allowed"
			case 3159:
				detail += ": secure transport is required"
			}
		}
	}
	return fmt.Errorf("connector %q (%s) connection failed: %s", name, driver, detail)
}

// Inspect only the known driver's public numeric/state fields. Importing the
// MySQL driver here would register it in otherwise driver-neutral executables.
func mysqlDiagnostic(err error) (uint64, string, bool) {
	for current, depth := err, 0; current != nil && depth < 32; current, depth = errors.Unwrap(current), depth+1 {
		value := reflect.ValueOf(current)
		if value.Kind() != reflect.Pointer || value.IsNil() {
			continue
		}
		value = value.Elem()
		if value.Kind() != reflect.Struct || value.Type().PkgPath() != "github.com/go-sql-driver/mysql" || value.Type().Name() != "MySQLError" {
			continue
		}
		number, state := value.FieldByName("Number"), value.FieldByName("SQLState")
		if number.Kind() != reflect.Uint16 || state.Kind() != reflect.Array || state.Len() != 5 {
			return 0, "", false
		}
		data := make([]byte, 5)
		for i := range data {
			data[i] = byte(state.Index(i).Uint())
			if !(data[i] >= '0' && data[i] <= '9' || data[i] >= 'A' && data[i] <= 'Z') {
				return number.Uint(), "unknown", true
			}
		}
		return number.Uint(), string(data), true
	}
	return 0, "", false
}
