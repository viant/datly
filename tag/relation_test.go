package tag

import "testing"

func TestParseRelation(t *testing.T) {
	actual, err := ParseRelation("TenantKey:p.tenant_id(true)=TenantID:c.tenant_id,account_key=account_key")
	if err != nil {
		t.Fatalf("ParseRelation() error = %v", err)
	}
	if len(actual) != 2 || actual[0].Parent.Field != "TenantKey" || actual[0].Parent.Namespace != "p" || actual[0].Parent.Column != "tenant_id" ||
		actual[0].Parent.Include == nil || !*actual[0].Parent.Include || actual[0].Child.Field != "TenantID" ||
		actual[0].Child.Namespace != "c" || actual[0].Child.Column != "tenant_id" || actual[1].Parent.Column != "account_key" || actual[1].Child.Column != "account_key" {
		t.Fatalf("ParseRelation() = %+v", actual)
	}
}

func TestParseRelationRejectsMalformedLinks(t *testing.T) {
	for _, value := range []string{
		"missing",
		"a=b=c",
		"a:=b",
		"a:b:c=d",
		"a=(true)",
		"a=b(true)",
		"a(maybe)=b",
		"a..b=c",
		"a=b.",
	} {
		t.Run(value, func(t *testing.T) {
			if _, err := ParseRelation(value); err == nil {
				t.Fatalf("ParseRelation(%q) expected error", value)
			}
		})
	}
}
