package builder

import (
	"reflect"
	"testing"

	xstate "github.com/viant/xdatly/state"
)

func TestSelectorCriteriaHelpers(t *testing.T) {
	t.Run("bind selector criteria no token unused positional", func(t *testing.T) {
		_, _, err := bindSelectorCriteriaSQL("SELECT * FROM users WHERE id = :ID", parameterResolver(map[string]any{"id": 7}), nil, []any{1})
		if err == nil {
			t.Fatalf("expected unused positional placeholder error")
		}
	})

	t.Run("bind selector criteria explicit token missing prefix placeholder", func(t *testing.T) {
		selector := &xstate.Selector{Criteria: "active = 1"}
		_, _, err := bindSelectorCriteriaSQL("SELECT * FROM users WHERE id = :Missing $SELECTOR_CRITERIA", nil, selector, nil)
		if err == nil {
			t.Fatalf("expected missing prefix placeholder error")
		}
	})

	t.Run("bind selector criteria explicit token unused positional after suffix", func(t *testing.T) {
		selector := &xstate.Selector{Criteria: "active = 1"}
		_, _, err := bindSelectorCriteriaSQL("SELECT * FROM users $SELECTOR_CRITERIA", nil, selector, []any{1})
		if err == nil {
			t.Fatalf("expected explicit-token unused positional error")
		}
	})

	t.Run("bind selector criteria explicit token missing suffix placeholder", func(t *testing.T) {
		selector := &xstate.Selector{Criteria: "active = 1"}
		_, _, err := bindSelectorCriteriaSQL("SELECT * FROM users $SELECTOR_CRITERIA AND id = :Missing", nil, selector, nil)
		if err == nil {
			t.Fatalf("expected explicit-token missing suffix placeholder error")
		}
	})

	t.Run("selector token skips protected SQL text", func(t *testing.T) {
		selector := &xstate.Selector{Criteria: "active = ?", Placeholders: []any{true}}
		source := "SELECT '$SELECTOR_CRITERIA' AS marker FROM users WHERE 1=1 $AND_SELECTOR_CRITERIA -- $SELECTOR_CRITERIA"
		actualSQL, actualArgs, err := bindSelectorCriteriaSQL(source, nil, selector, nil)
		if err != nil {
			t.Fatalf("unexpected protected selector bind error: %v", err)
		}
		expectSQL := "SELECT '$SELECTOR_CRITERIA' AS marker FROM users WHERE 1=1  AND active = ? -- $SELECTOR_CRITERIA"
		if actualSQL != expectSQL || !reflect.DeepEqual(actualArgs, []any{true}) {
			t.Fatalf("unexpected protected selector result: %q %#v", actualSQL, actualArgs)
		}
	})

	t.Run("append auto selector criteria guard paths", func(t *testing.T) {
		sqlText, args := appendAutoSelectorCriteria("SELECT * FROM users", nil, nil, false)
		if sqlText != "SELECT * FROM users" || args != nil {
			t.Fatalf("unexpected nil-selector auto criteria result: %q %#v", sqlText, args)
		}

		selector := &xstate.Selector{Criteria: "name = ?", Placeholders: []any{"john"}}
		sqlText, args = appendAutoSelectorCriteria("SELECT * FROM users", []any{7}, selector, true)
		if sqlText != "SELECT * FROM users" || len(args) != 1 || args[0] != 7 {
			t.Fatalf("unexpected explicit-token auto criteria guard result: %q %#v", sqlText, args)
		}

		sqlText, args = appendAutoSelectorCriteria("SELECT * FROM users", nil, &xstate.Selector{}, false)
		if sqlText != "SELECT * FROM users" || args != nil {
			t.Fatalf("unexpected empty-criteria auto criteria result: %q %#v", sqlText, args)
		}
	})

	t.Run("append auto selector criteria appends where and args", func(t *testing.T) {
		selector := &xstate.Selector{Criteria: "name = ?", Placeholders: []any{"john"}}
		sqlText, args := appendAutoSelectorCriteria("SELECT * FROM users WHERE tenant_id = ? ORDER BY id", []any{7}, selector, false)
		if sqlText != "SELECT * FROM users WHERE tenant_id = ? AND name = ? ORDER BY id" {
			t.Fatalf("unexpected appended selector criteria SQL: %q", sqlText)
		}
		expectArgs := []any{7, "john"}
		if !reflect.DeepEqual(args, expectArgs) {
			t.Fatalf("unexpected appended selector criteria args: %#v", args)
		}
	})

	t.Run("append auto selector criteria with no placeholders leaves args unchanged", func(t *testing.T) {
		selector := &xstate.Selector{Criteria: "active = 1"}
		sqlText, args := appendAutoSelectorCriteria("SELECT * FROM users WHERE tenant_id = ? ORDER BY id", []any{7}, selector, false)
		if sqlText != "SELECT * FROM users WHERE tenant_id = ? AND active = 1 ORDER BY id" {
			t.Fatalf("unexpected appended no-placeholder selector SQL: %q", sqlText)
		}
		if !reflect.DeepEqual(args, []any{7}) {
			t.Fatalf("unexpected appended no-placeholder selector args: %#v", args)
		}
	})

	t.Run("append auto selector criteria preserves argument order before having", func(t *testing.T) {
		selector := &xstate.Selector{Criteria: "tenant_id > ?", Placeholders: []any{1}}
		sqlText, args := appendAutoSelectorCriteria(
			"SELECT tenant_id, COUNT(*) AS total FROM users GROUP BY tenant_id HAVING COUNT(*) > ? ORDER BY tenant_id",
			[]any{2}, selector, false,
		)
		expectSQL := "SELECT tenant_id, COUNT(*) AS total FROM users WHERE tenant_id > ? GROUP BY tenant_id HAVING COUNT(*) > ? ORDER BY tenant_id"
		if sqlText != expectSQL {
			t.Fatalf("unexpected grouped selector SQL: %q", sqlText)
		}
		if !reflect.DeepEqual(args, []any{1, 2}) {
			t.Fatalf("unexpected grouped selector argument order: %#v", args)
		}
	})

	t.Run("insert selector clause appends when no ordering clauses", func(t *testing.T) {
		if actual := insertSelectorClause("SELECT * FROM users", " WHERE active = 1"); actual != "SELECT * FROM users WHERE active = 1" {
			t.Fatalf("unexpected appended selector clause: %q", actual)
		}
	})
}
