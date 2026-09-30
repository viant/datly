package compile

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/parsly"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	"github.com/viant/velty/ast"
	velexpr "github.com/viant/velty/ast/expr"
	"github.com/viant/velty/ast/stmt"
	veltyparser "github.com/viant/velty/parser"
)

// readTemplateSource separates SQL metadata analysis from executable template
// statements. Native Velty statement ranges receive unique comment identities;
// parser-owned raw sources carry them through canonical view decomposition.
type readTemplateSource struct {
	AnalysisSQL, original string
	fragments             []readTemplateFragment
}
type readTemplateFragment struct {
	marker, closing, text, body string
	begin, end                  int
	terminal                    bool
}

func newReadTemplateSource(source string) (*readTemplateSource, error) {
	result := &readTemplateSource{original: source}
	prefix := "__datly_read_template_"
	for strings.Contains(source, prefix) {
		prefix = "_" + prefix
	}
	var analysis strings.Builder
	for offset := 0; offset < len(source); {
		end := strings.IndexByte(source[offset:], '\n')
		if end < 0 {
			end = len(source)
		} else {
			end += offset + 1
		}
		line := source[offset:end]
		trimmed := strings.TrimLeft(line, " \t\r")
		if executableReadStatement(trimmed) {
			begin := offset + len(line) - len(trimmed)
			cursor := parsly.NewCursor("reader SQL", []byte(source), 0)
			cursor.Pos = begin
			if _, err := veltyparser.MatchStatement(cursor); err != nil {
				return nil, &Error{Code: CodeSQLParse, Offset: begin, Cause: err}
			}
			marker := fmt.Sprintf("/*%s%d__*/", prefix, len(result.fragments))
			text := source[begin:cursor.Pos]
			body, err := readTemplateAnalysisBody(text)
			if err != nil {
				return nil, &Error{Code: CodeSQLParse, Offset: begin, Cause: err}
			}
			closing := ""
			if strings.TrimSpace(body) != "" {
				closing = strings.Replace(marker, "_template_", "_template_end_", 1)
			}
			result.fragments = append(result.fragments, readTemplateFragment{marker: marker, closing: closing, text: text, body: body, begin: begin, end: cursor.Pos, terminal: strings.TrimSpace(source[cursor.Pos:]) == ""})
			analysis.WriteString(source[offset:begin])
			analysis.WriteString(marker)
			analysis.WriteString(body)
			analysis.WriteString(closing)
			offset = cursor.Pos
		} else {
			analysis.WriteString(line)
			offset = end
		}
	}
	result.AnalysisSQL = analysis.String()
	return result, nil
}
func executableReadStatement(source string) bool {
	for _, name := range []string{"else", "end"} {
		prefix := "#" + name
		if strings.HasPrefix(source, prefix) && (len(source) == len(prefix) || source[len(prefix)] == ' ' || source[len(prefix)] == '\t' || source[len(prefix)] == '\n' || source[len(prefix)] == '\r') {
			return true
		}
	}
	for _, name := range []string{"if", "elseif", "foreach", "for", "set", "evaluate"} {
		if strings.HasPrefix(source, "#"+name+"(") {
			return true
		}
	}
	return false
}
func (s *readTemplateSource) restore(root *spec.View, parsed *query.Select, frame TemplateFrame) error {
	if len(s.fragments) == 0 {
		return nil
	}
	// Direct physical-table readers lower only their outer projection metadata.
	// Preserve the authored FROM/WHERE/window program rather than serializing
	// its analysis skeleton, whose control flow is not represented by SQL AST.
	if len(root.Relations) == 0 && root.Source != nil && parsed != nil {
		switch parsed.From.X.(type) {
		case *expr.Ident, *expr.Selector:
			originalFrom := topLevelReadFrom(s.original)
			loweredFrom := topLevelReadFrom(root.Source.SQL)
			if originalFrom >= 0 && loweredFrom >= 0 && root.Source.SQL != s.original {
				root.Source.SQL = root.Source.SQL[:loweredFrom] + s.original[originalFrom:]
				if frame.Suffix != "" {
					root.Source.SQL += "\n" + frame.Suffix
				}
			}
		}
	}
	found := make(map[string]bool, len(s.fragments))
	var visit func(*spec.View)
	visit = func(view *spec.View) {
		if view == nil {
			return
		}
		if view.Source != nil {
			fullOriginal := strings.Contains(view.Source.SQL, s.original)
			from := topLevelReadFrom(s.original)
			directSuffix := view == root && from >= 0 && strings.Contains(view.Source.SQL, s.original[from:])
			for _, fragment := range s.fragments {
				if fullOriginal || (directSuffix && fragment.begin >= from) {
					found[fragment.marker] = true
				}
			}
			for _, fragment := range s.fragments {
				// A terminal executable statement belongs after the complete query. The
				// SQL parser may attach its analysis comment to the final ORDER operand;
				// restoring there would put it before an implicit ASC/DESC direction.
				if view == root && fragment.terminal && strings.TrimSpace(fragment.body) == "" && strings.Contains(view.Source.SQL, fragment.marker) {
					view.Source.SQL = strings.ReplaceAll(view.Source.SQL, fragment.marker, "")
					continue
				}
				if start := strings.Index(view.Source.SQL, fragment.marker); start >= 0 {
					finish := start + len(fragment.marker)
					if fragment.closing != "" {
						closing := strings.Index(view.Source.SQL[finish:], fragment.closing)
						if closing < 0 {
							continue
						}
						finish += closing + len(fragment.closing)
					}
					view.Source.SQL = view.Source.SQL[:start] + fragment.text + view.Source.SQL[finish:]
					found[fragment.marker] = true
				}

			}
		}
		if view.Source != nil {
			view.Source.Embeds = dql.EmbeddedSQLRefs(view.Source.SQL)
		}
		for _, relation := range view.Relations {
			if relation != nil {
				visit(relation.View)
			}
		}
	}
	visit(root)
	for _, fragment := range s.fragments {
		if found[fragment.marker] {
			continue
		}
		// SQLParser does not represent a terminal executable statement in its
		// SELECT AST. Its native parser range establishes an outer source suffix;
		// restore that suffix after a root projection rewrite, never on children.
		if fragment.terminal && strings.TrimSpace(fragment.body) == "" && root.Source != nil && strings.TrimSpace(root.Source.SQL) != "" {
			root.Source.SQL = strings.TrimRight(root.Source.SQL, " \t\r\n") + "\n" + fragment.text
			root.Source.Embeds = dql.EmbeddedSQLRefs(root.Source.SQL)
			continue
		}
		return &Error{Code: CodeSQLParse, Offset: fragment.begin, Cause: fmt.Errorf("SQL rewrite lost an executable template statement")}
	}
	return nil
}

// Metadata still sees SQL written inside every branch. Only executable control
// syntax and callable template outputs are opaque; masking a whole branch
// would hide nested view controls and relation/column authority.
func readTemplateAnalysisBody(source string) (string, error) {
	root, spans, err := veltyparser.ParseWithSpansDetailed([]byte(source))
	if err != nil {
		return "", err
	}
	var result strings.Builder
	var visit func(ast.Statement)
	visit = func(node ast.Statement) {
		switch n := node.(type) {
		case *stmt.Append:
			result.WriteString(n.Append)
		case *velexpr.Select:
			if !readTemplateSelectorCall(n) {
				if span, ok := spans[n]; ok && span.Start >= 0 && span.End < len(source) {
					result.WriteString(source[span.Start : span.End+1])
				}
			}
		case *stmt.If:
			for _, item := range n.Body.Statements() {
				visit(item)
			}
			if n.Else != nil {
				visit(n.Else)
			}
		case ast.StatementContainer:
			for _, item := range n.Statements() {
				visit(item)
			}
		}
	}
	for _, node := range root.Statements() {
		visit(node)
	}
	return result.String(), nil
}

// topLevelReadFrom identifies the lexical range of the physical root source.
// SQLParser's structural FROM node must authorize using this range; comments,
// quoted identifiers/literals, CTEs and projection subqueries are skipped.
func topLevelReadFrom(sql string) int {
	depth := 0
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		if c == '$' {
			cursor := parsly.NewCursor("reader SQL", []byte(sql), 0)
			cursor.Pos = i + 1
			if selector, err := veltyparser.MatchSelector(cursor); err == nil && selector != nil {
				i = cursor.Pos - 1
				continue
			}
		}
		if c == '\'' || c == '"' || c == '`' || c == '[' {
			closing := c
			if c == '[' {
				closing = ']'
			}
			for i++; i < len(sql); i++ {
				if sql[i] == '\\' {
					i++
					continue
				}
				if sql[i] == closing {
					if i+1 < len(sql) && sql[i+1] == closing {
						i++
						continue
					}
					break
				}
			}
			continue
		}
		if c == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			continue
		}
		if c == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			if end := strings.Index(sql[i+2:], "*/"); end >= 0 {
				i += end + 3
				continue
			}
			return -1
		}
		if c == '(' {
			depth++
			continue
		}
		if c == ')' {
			depth--
			continue
		}
		if depth == 0 && i+4 <= len(sql) && strings.EqualFold(sql[i:i+4], "from") && (i == 0 || !readIdentifierByte(sql[i-1])) && (i+4 == len(sql) || !readIdentifierByte(sql[i+4])) {
			return i
		}
	}
	return -1
}
func readIdentifierByte(c byte) bool {
	return c >= 128 || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '.'
}

func readTemplateSelectorCall(node *velexpr.Select) bool {
	if node == nil {
		return false
	}
	switch next := node.X.(type) {
	case *velexpr.Call:
		return true
	case *velexpr.Select:
		return readTemplateSelectorCall(next)
	}
	return false
}
