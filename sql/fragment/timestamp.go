package fragment

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
)

// TimestampSecondsUTC renders an explicit UTC whole-second timestamp key.
// It removes fractional seconds before conversion so a fraction cannot round
// across a second boundary. Null and malformed values produce SQL NULL.
func (c *Context) TimestampSecondsUTC(expression string) (string, error) {
	p, err := c.timestampParts(expression)
	if err != nil {
		return "", err
	}
	var result string
	if p.sqlite {
		result = "DATETIME(" + p.base + ", CAST((-(" + p.offset + ")) AS TEXT) || ' minutes')"
	} else {
		result = "DATE_FORMAT(DATE_SUB(CAST(" + p.base + " AS DATETIME), INTERVAL (" + p.offset + ") MINUTE), '%Y-%m-%d %H:%i:%s')"
	}
	return "(CASE WHEN " + p.valid + " THEN " + result + " ELSE NULL END)", nil
}

// TimestampNanoseconds renders the exact first nine fractional digits as an
// integer key. The SQL conversion never includes a timezone suffix.
func (c *Context) TimestampNanoseconds(expression string) (string, error) {
	p, err := c.timestampParts(expression)
	if err != nil {
		return "", err
	}
	numeric, integer := "NUMERIC", "INTEGER"
	if !p.sqlite {
		numeric, integer = "DECIMAL(20,9)", "SIGNED"
	}
	fraction := "CASE WHEN " + p.body + " = '' THEN 0 ELSE CAST(ROUND(CAST(SUBSTR(" + p.body + ",1,10) AS " + numeric + ") * 1000000000) AS " + integer + ") END"
	return "(CASE WHEN " + p.valid + " THEN " + fraction + " ELSE NULL END)", nil
}

type timestampParts struct {
	sqlite                    bool
	base, body, offset, valid string
}

func (c *Context) timestampParts(expression string) (*timestampParts, error) {
	if c == nil || c.dialect == nil {
		return nil, fmt.Errorf("timestamp key requires a dialect")
	}
	dialect := strings.ToLower(strings.TrimSpace(c.dialect.Name))
	isSQLite := dialect == "sqlite" || dialect == "sqlite3"
	if !isSQLite && dialect != "mysql" {
		return nil, fmt.Errorf("timestamp key is unsupported for dialect %q", c.dialect.Name)
	}
	source, err := timestampExpression(expression)
	if err != nil {
		return nil, err
	}
	text := "TRIM(CAST(" + source + " AS CHAR))"
	base := "REPLACE(SUBSTR(" + text + ",1,19),'T',' ')"
	standard := "(SUBSTR(" + text + ",-6,1) IN ('+','-') AND SUBSTR(" + text + ",-3,1) = ':')"
	// Go's database time string has an explicit numeric offset and zone label.
	compact := "(SUBSTR(" + text + ",-9,1) IN ('+','-') AND SUBSTR(" + text + ",-4,1) = ' ')"
	z := "SUBSTR(" + text + ",-1,1) = 'Z'"
	body := "TRIM(SUBSTR(" + text + ",20,CASE WHEN " + standard + " THEN LENGTH(" + text + ")-25 WHEN " + compact + " THEN LENGTH(" + text + ")-29 WHEN " + z + " THEN LENGTH(" + text + ")-20 ELSE LENGTH(" + text + ")-19 END))"
	standardOffset := "(SUBSTR(" + text + ",-5,2)*60 + SUBSTR(" + text + ",-2,2)) * CASE WHEN SUBSTR(" + text + ",-6,1) = '-' THEN -1 ELSE 1 END"
	compactOffset := "(SUBSTR(" + text + ",-8,2)*60 + SUBSTR(" + text + ",-6,2)) * CASE WHEN SUBSTR(" + text + ",-9,1) = '-' THEN -1 ELSE 1 END"
	offset := "CASE WHEN " + standard + " THEN " + standardOffset + " WHEN " + compact + " THEN " + compactOffset + " ELSE 0 END"
	var baseValid, fractionValid, standardValid, compactValid string
	if isSQLite {
		baseValid = "DATETIME(" + base + ",'+0 days') = " + base + " AND SUBSTR(" + text + ",1,4) > '0000'"
		fractionValid = "(" + body + " = '' OR (SUBSTR(" + body + ",1,1) = '.' AND LENGTH(" + body + ") > 1 AND SUBSTR(" + body + ",2) NOT GLOB '*[^0-9]*'))"
		standardValid = "SUBSTR(" + text + ",-6) GLOB '[+-][0-9][0-9]:[0-9][0-9]'"
		compactValid = "SUBSTR(" + text + ",-10) GLOB ' [+-][0-9][0-9][0-9][0-9] [A-Z][A-Z][A-Z]'"
	} else {
		baseValid = "DATE_FORMAT(CAST(" + base + " AS DATETIME),'%Y-%m-%d %H:%i:%s') = " + base + " AND SUBSTR(" + text + ",1,4) > '0000' AND DAYOFMONTH(CAST(" + base + " AS DATETIME)) <= DAYOFMONTH(LAST_DAY(CAST(" + base + " AS DATETIME)))"
		fractionValid = "(" + body + " = '' OR REGEXP_LIKE(" + body + ",'^[.][0-9]+$'))"
		standardValid = "REGEXP_LIKE(SUBSTR(" + text + ",-6),'^[+-][0-9]{2}:[0-9]{2}$')"
		compactValid = "REGEXP_LIKE(SUBSTR(" + text + ",-10),'^ [+-][0-9]{4} [A-Z]{3}$')"
	}
	zoneValid := "((NOT " + standard + " AND NOT " + compact + ") OR (" + standard + " AND " + standardValid + " AND SUBSTR(" + text + ",-5,2)+0 < 24 AND SUBSTR(" + text + ",-2,2)+0 < 60) OR (" + compact + " AND " + compactValid + " AND SUBSTR(" + text + ",-8,2)+0 < 24 AND SUBSTR(" + text + ",-6,2)+0 < 60))"
	return &timestampParts{sqlite: isSQLite, base: base, body: body, offset: offset, valid: "(" + baseValid + ") AND " + fractionValid + " AND " + zoneValid}, nil
}

func timestampExpression(input string) (string, error) {
	input = strings.TrimSpace(input)
	if input == "" || len(input) > 512 {
		return "", fmt.Errorf("timestamp expression is empty or exceeds 512 bytes")
	}
	for _, r := range input {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != '.' && r != ',' && r != '(' && r != ')' && r != ' ' && r != '\t' {
			return "", fmt.Errorf("timestamp expression contains unsupported syntax")
		}
	}
	parsed, err := sqlparser.ParseQuery("SELECT "+input, sqlparser.WithStructuralValidation())
	if err != nil {
		return "", fmt.Errorf("parse timestamp expression: %w", err)
	}
	if len(parsed.List) != 1 || parsed.List[0].Alias != "" || parsed.From.X != nil || len(parsed.Joins) > 0 || parsed.Qualify != nil || len(parsed.OrderBy) > 0 || len(parsed.GroupBy) > 0 || parsed.Having != nil || parsed.Limit != nil || parsed.Offset != nil || parsed.Union != nil || len(parsed.WithSelects) > 0 {
		return "", fmt.Errorf("timestamp requires a scalar column or COALESCE expression")
	}
	if !timestampNode(parsed.List[0].Expr, 0) {
		return "", fmt.Errorf("timestamp requires bounded column references and COALESCE")
	}
	return sqlparser.Stringify(parsed.List[0].Expr), nil
}
func timestampNode(value node.Node, depth int) bool {
	if depth > 4 {
		return false
	}
	validName := func(name string) bool {
		if name == "" {
			return false
		}
		for i, r := range name {
			if r != '_' && !unicode.IsLetter(r) && (i == 0 || !unicode.IsDigit(r)) {
				return false
			}
		}
		return true
	}
	switch actual := value.(type) {
	case *expr.Ident:
		return validName(actual.Name)
	case *expr.Selector:
		return actual.Expression == "" && validName(actual.Name) && timestampNode(actual.X, depth+1)
	case *expr.Call:
		name, ok := actual.X.(*expr.Ident)
		if !ok || !strings.EqualFold(name.Name, "COALESCE") || len(actual.Args) < 2 || len(actual.Args) > 8 {
			return false
		}
		for _, arg := range actual.Args {
			if !timestampNode(arg, depth+1) {
				return false
			}
		}
		return true
	}
	return false
}

// TimestampAnalysisExpression keeps the physical input columns and scalar
// return type visible to SQL metadata analysis without choosing a dialect.
// The executable source retains the explicit native renderer call.
func TimestampAnalysisExpression(method, expression string) (string, error) {
	source, err := timestampExpression(expression)
	if err != nil {
		return "", err
	}
	switch method {
	case "TimestampSecondsUTC":
		return "CAST(" + source + " AS CHAR)", nil
	case "TimestampNanoseconds":
		return "CAST(" + source + " AS BIGINT)", nil
	default:
		return "", fmt.Errorf("unsupported timestamp analysis method %q", method)
	}
}
