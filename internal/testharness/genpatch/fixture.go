// Package genpatch owns the shared high-level generation acceptance fixture.
package genpatch

import (
	_ "embed"
	"strings"
)

var Schema = []string{
	`PRAGMA foreign_keys=ON`,
	`CREATE TABLE ORDERS(ID INTEGER PRIMARY KEY AUTOINCREMENT, KIND_ID INTEGER NOT NULL REFERENCES ORDER_KINDS(ID), NAME TEXT NOT NULL, START DATETIME NOT NULL, END DATETIME NOT NULL)`,
	`CREATE TABLE ITEMS(ID INTEGER PRIMARY KEY AUTOINCREMENT, ORDER_ID INTEGER NOT NULL REFERENCES ORDERS(ID), NAME TEXT NOT NULL)`,
	`CREATE TABLE ORDER_KINDS(ID INTEGER PRIMARY KEY, NAME TEXT NOT NULL)`,
	`INSERT INTO ORDER_KINDS VALUES(7,'standard'),(1,'decoy')`,
	`INSERT INTO ORDERS VALUES(1,7,'before','2026-09-01T00:00:00Z','2026-09-30T00:00:00Z')`,
	`INSERT INTO ITEMS VALUES(10,1,'old')`,
	`CREATE TRIGGER no_kind_insert BEFORE INSERT ON ORDER_KINDS BEGIN SELECT RAISE(ABORT,'auxiliary insert'); END`,
	`CREATE TRIGGER no_kind_update BEFORE UPDATE ON ORDER_KINDS BEGIN SELECT RAISE(ABORT,'auxiliary update'); END`,
	`CREATE TRIGGER no_kind_delete BEFORE DELETE ON ORDER_KINDS BEGIN SELECT RAISE(ABORT,'auxiliary delete'); END`,
}

const PackageDirective = `#package('api/orders')`

const DQL = PackageDirective + `
#setting($_ = $route('/orders','PATCH'))
SELECT o.*, Items.*, Kinds.*,
 tag(o.START,'invariant:"Interval"'),
 tag(o.END,'invariant:"Interval" validate:"gtfield(Start)"')
FROM ORDERS o
JOIN ITEMS Items ON Items.ORDER_ID = o.ID
JOIN (ORDER_KINDS) Kinds ON Kinds.ID = o.KIND_ID`

//go:embed runtime.go.txt
var RuntimeSource string

//go:embed batch_runtime.go.txt
var BatchRuntimeSource string

// LifecycleDQL explicitly requests application hooks for the writable roles.
var LifecycleDQL = strings.Replace(DQL, "SELECT o.*, Items.*, Kinds.*,", "SELECT o.*, Items.*, Kinds.*, lifecycle_type(o,'OrderRules'), lifecycle_type(Items,'ItemRules'),", 1)
