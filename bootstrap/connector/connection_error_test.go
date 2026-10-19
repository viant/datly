package connector

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"net"
	"strings"
	"syscall"
	"testing"
)

func TestConnectionDiagnosticsDoNotLeakDriverMessages(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{&mysql.MySQLError{Number: 1044, SQLState: [5]byte{'4', '2', '0', '0', '0'}, Message: "password=secret DSN=private"}, "1044 (SQLSTATE 42000): database access denied"},
		{&mysql.MySQLError{Number: 1045, SQLState: [5]byte{'2', '8', '0', '0', '0'}, Message: "password=secret"}, "authentication rejected"},
		{&net.DNSError{Err: "secret", Name: "private"}, "DNS lookup failed"},
		{context.DeadlineExceeded, "timed out"}, {syscall.ECONNREFUSED, "connection refused"},
		{errors.New("password=secret DSN=private"), "database rejected"},
	} {
		got := connectionError("studio", "mysql", fmt.Errorf("wrapped secret: %w", tc.err)).Error()
		if !strings.Contains(got, tc.want) || strings.Contains(got, "secret") || strings.Contains(got, "private") {
			t.Fatalf("unexpected diagnostic: %s", got)
		}
	}
}
