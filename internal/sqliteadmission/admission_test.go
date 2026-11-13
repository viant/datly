package sqliteadmission

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSharedFileAdmissionCancellationAndLifetime(t *testing.T) {
	a, b := new(sql.DB), new(sql.DB)
	path := filepath.Join(t.TempDir(), "test.db")
	closeA, err := Register(a, "sqlite", "file:"+path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	closeB, err := Register(b, "sqlite3", path+"?_busy_timeout=5000")
	if err != nil {
		t.Fatal(err)
	}
	first, err := Acquire(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err = Acquire(ctx, b); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter error=%v", err)
	}
	closeA()
	closeB()
	first()
	first()
	registry.Lock()
	defer registry.Unlock()
	if len(registry.handles) != 0 || len(registry.gates) != 0 {
		t.Fatal("admission registry leaked")
	}
}
func TestDifferentDatabasesRemainIndependent(t *testing.T) {
	a, b := new(sql.DB), new(sql.DB)
	dir := t.TempDir()
	ca, _ := Register(a, "sqlite", filepath.Join(dir, "a.db"))
	defer ca()
	cb, _ := Register(b, "sqlite", filepath.Join(dir, "b.db"))
	defer cb()
	ra, e := Acquire(context.Background(), a)
	if e != nil {
		t.Fatal(e)
	}
	defer ra()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rb, e := Acquire(ctx, b)
	if e != nil {
		t.Fatal(e)
	}
	rb()
}

func TestDatabaseIdentityFileURIsMemoryAndAliases(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a b.db")
	a, b := new(sql.DB), new(sql.DB)
	plain, e := identity(a, path)
	if e != nil {
		t.Fatal(e)
	}
	for _, dsn := range []string{(&url.URL{Scheme: "file", Path: path}).String(), (&url.URL{Scheme: "file", Host: "localhost", Path: path}).String()} {
		got, e := identity(b, dsn)
		if e != nil || got != plain {
			t.Fatalf("URI identity %q error=%v", got, e)
		}
	}
	literal, e := identity(a, filepath.Join(dir, "a%20b.db"))
	if e != nil || literal == plain {
		t.Fatal("plain filename was URL-decoded")
	}
	alias := filepath.Join(dir, "alias")
	if e = os.Symlink(dir, alias); e != nil {
		t.Fatal(e)
	}
	got, e := identity(a, filepath.Join(alias, "a b.db"))
	if e != nil || got != plain {
		t.Fatal("parent symlink identity mismatch")
	}
	one, _ := identity(a, ":memory:")
	two, _ := identity(b, ":memory:")
	if one == two {
		t.Fatal("private memory databases shared admission")
	}
	one, _ = identity(a, "file:shared?mode=memory&cache=shared")
	two, _ = identity(b, "file:shared?cache=shared&mode=memory")
	if one != two {
		t.Fatal("shared memory identity mismatch")
	}
}
