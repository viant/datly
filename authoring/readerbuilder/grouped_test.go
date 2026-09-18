package readerbuilder

import (
	"context"
	"strings"
	"testing"
)

func TestCubeSettingRequiresSimpleGroupedMainView(t *testing.T) {
	grouped := `#setting($_ = $route('/spend','GET'))
SELECT spend.*, groupable(spend), tag(spend.account_id,'groupable:"true"'), CAST(spend.total AS float64)
FROM (SELECT s.account_id,SUM(s.amount) AS total FROM spend s GROUP BY s.account_id) spend`
	response := New(Config{Name: "Spend"}).Apply(context.Background(), Request{DQL: grouped, Operation: Operation{
		Type: OperationSetSetting, Setting: &SettingMutation{Name: "cube", Args: []string{}},
	}})
	if !response.Applied || response.Structure.Component.Settings == nil || response.Structure.Component.Settings.Report == nil || !response.Structure.Component.Settings.Report.Enabled {
		t.Fatalf("response=%+v", response)
	}
	plain := strings.Replace(grouped, `,SUM(s.amount) AS total`, `,s.amount AS total`, 1)
	response = New(Config{Name: "Spend"}).Apply(context.Background(), Request{DQL: plain, Operation: Operation{
		Type: OperationSetSetting, Setting: &SettingMutation{Name: "cube", Args: []string{}},
	}})
	if response.Applied || response.DQL != plain {
		t.Fatalf("response=%+v", response)
	}
}

func TestComposeSettingRequiresEnabledCubeAndCarriesMCPBudgets(t *testing.T) {
	grouped := `#setting($_ = $route('/spend','GET'))
SELECT spend.*, groupable(spend), tag(spend.account_id,'groupable:"true"'), CAST(spend.total AS float64)
FROM (SELECT s.account_id,SUM(s.amount) AS total FROM spend s GROUP BY s.account_id) spend`
	service := New(Config{Name: "Spend"})
	rejected := service.Apply(context.Background(), Request{DQL: grouped, Operation: Operation{
		Type: OperationSetSetting, Setting: &SettingMutation{Name: "cubeCompose", Args: []string{"true", "true", "4", "50", "12000"}},
	}})
	if rejected.Applied {
		t.Fatalf("compose without cube=%+v", rejected)
	}
	withCube := strings.Replace(grouped, "SELECT spend.*", "#setting($_ = $cube())\nSELECT spend.*", 1)
	response := service.Apply(context.Background(), Request{DQL: withCube, Operation: Operation{
		Type: OperationSetSetting, Setting: &SettingMutation{Name: "cubeCompose", Args: []string{"true", "true", "4", "50", "12000"}},
	}})
	if !response.Applied {
		t.Fatalf("response=%+v", response)
	}
	compose := response.Structure.Component.Settings.Report.Compose
	if compose == nil || compose.MCPTool == nil || !*compose.MCPTool || compose.MaxCubes != 4 || compose.MaxLimit != 50 || compose.TimeoutMs != 12000 {
		t.Fatalf("compose=%+v", compose)
	}
}

func TestServiceColumnRolePreservesOtherTags(t *testing.T) {
	source := `#setting($_ = $route('/spend','GET'))
SELECT summary.*,groupable(summary),tag(summary.status,'json:"statusName" groupable:"true"'),CAST(summary.total AS float64)
FROM (SELECT status,COUNT(*) AS total FROM spend GROUP BY status) summary`
	service := New(Config{Name: "Spend"})
	measure := service.Apply(context.Background(), Request{DQL: source, Operation: Operation{Type: OperationSetColumnRole, ColumnRole: &ColumnRoleMutation{View: "summary", Column: "status", Role: "measure"}}})
	if !measure.Applied || !strings.Contains(measure.DQL, `groupable:"false"`) || !strings.Contains(measure.DQL, `json:"statusName"`) {
		t.Fatalf("measure=%+v", measure)
	}
	dimension := service.Apply(context.Background(), Request{DQL: measure.DQL, Operation: Operation{Type: OperationSetColumnRole, ColumnRole: &ColumnRoleMutation{View: "summary", Column: "status", Role: "dimension"}}})
	if !dimension.Applied || !strings.Contains(dimension.DQL, `groupable:"true"`) || !strings.Contains(dimension.DQL, `json:"statusName"`) {
		t.Fatalf("dimension=%+v", dimension)
	}
}
