package genpatch

import _ "embed"

var CompositeSchema = []string{
	"PRAGMA foreign_keys=ON",
	"CREATE TABLE ORDERS(TENANT_ID INTEGER NOT NULL,ID INTEGER NOT NULL,KIND_TENANT_ID INTEGER,KIND_ID INTEGER,NAME TEXT,PRIMARY KEY(TENANT_ID,ID))",
	"CREATE TABLE ORDER_KINDS(TENANT_ID INTEGER NOT NULL,ID INTEGER NOT NULL,NAME TEXT,PRIMARY KEY(TENANT_ID,ID))",
	"INSERT INTO ORDERS VALUES(1,5,10,7,'a'),(2,5,20,8,'b')",
	"INSERT INTO ORDER_KINDS VALUES(10,7,'a'),(10,8,'hybrid'),(20,7,'hybrid'),(20,8,'b')",
}

const CompositeDQL = PackageDirective + `
#setting($_ = $route('/orders','PATCH'))
SELECT o.*,Kinds.* FROM ORDERS o JOIN (ORDER_KINDS) Kinds ON Kinds.TENANT_ID=o.KIND_TENANT_ID AND Kinds.ID=o.KIND_ID`

//go:embed composite_runtime.go.txt
var CompositeRuntimeSource string
