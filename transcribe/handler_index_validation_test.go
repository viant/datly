package transcribe

import "testing"

func TestVeltyHandlerIndexedSelectorValidation(t *testing.T) {
	for _, test := range []struct {
		name, template string
		valid          bool
	}{
		{"field continuation", `$dml.Insert("ROWS", $Input.Rows[0].Children[0])`, true},
		{"loop index", `#foreach($row in $Input.Rows)#set($i = $foreach.Index)$dml.Insert("ROWS", $Input.Rows[$i].Children[0])#end`, true},
		{"unknown index", `$dml.Insert("ROWS", $Input.Rows[$unknown].Children[0])`, false},
		{"loop metadata outside scope", `$dml.Insert("ROWS", $Input.Rows[$foreach.Index])`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := validateVeltyHandlerTemplate(test.template, nil)
			if (err == nil) != test.valid {
				t.Fatalf("validation error = %v, valid = %v", err, test.valid)
			}
		})
	}
}
