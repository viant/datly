package parse

import (
	"errors"
	"strings"

	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlpre "github.com/viant/datly/repository/shape/dql/preprocess"
	"github.com/viant/datly/repository/shape/dql/shape"
	dqlstmt "github.com/viant/datly/repository/shape/dql/statement"
	"github.com/viant/parsly"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/query"
)

// Parser parses DQL source into a shape Document.
type Parser struct {
	options Options
}

// New creates a DQL parser.
func New(opts ...Option) *Parser {
	options := defaultOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	options.UnknownNonReadMode = normalizeUnknownNonReadMode(options.UnknownNonReadMode)
	return &Parser{options: options}
}

// Parse parses DQL and returns parsed document with diagnostics.
func (p *Parser) Parse(dql string) (*shape.Document, error) {
	doc := &shape.Document{Raw: dql}
	sql, ctx, directives, directiveDiagnostics := dqlpre.Extract(dql)
	doc.SQL = strings.TrimSpace(sql)
	doc.TypeContext = ctx
	doc.Directives = directives
	if len(directiveDiagnostics) > 0 {
		doc.Diagnostics = append(doc.Diagnostics, directiveDiagnostics...)
		for _, diagnostic := range directiveDiagnostics {
			if diagnostic != nil && diagnostic.Severity == shape.SeverityError {
				return doc, diagnostic
			}
		}
	}

	if doc.SQL == "" {
		d := &shape.Diagnostic{
			Code:     dqldiag.CodeParseEmpty,
			Severity: shape.SeverityError,
			Message:  "no SQL statement found",
			Hint:     "add SELECT/INSERT/UPDATE/DELETE statement after DQL directives",
			Span:     dqlpre.PointSpan(dql, 0),
		}
		doc.Diagnostics = append(doc.Diagnostics, d)
		return doc, d
	}

	statements := dqlstmt.New(sql)
	readStmt := firstReadStatement(statements)
	if readStmt == nil {
		if !hasExecStatement(statements) {
			severity := shape.SeverityWarning
			if p.options.UnknownNonReadMode == UnknownNonReadModeError {
				severity = shape.SeverityError
			}
			doc.Diagnostics = append(doc.Diagnostics, &shape.Diagnostic{
				Code:     dqldiag.CodeParseUnknownNonRead,
				Severity: severity,
				Message:  "no readable SELECT statement detected",
				Hint:     "use SELECT for read parsing or compile as DML/handler template",
				Span:     dqlpre.PointSpan(dql, 0),
			})
			if severity == shape.SeverityError {
				return doc, doc.Diagnostics[len(doc.Diagnostics)-1]
			}
		}
		// DML-only statement sets are valid for parse contract.
		return doc, nil
	}
	querySQL := sql[readStmt.Start:readStmt.End]
	queryNode, diag, err := parseQueryWithDiagnosticAt(querySQL, dql, readStmt.Start)
	if diag != nil {
		doc.Diagnostics = append(doc.Diagnostics, diag)
	}
	if err != nil {
		return doc, diag
	}
	doc.Query = queryNode
	return doc, nil
}

func firstReadStatement(statements dqlstmt.Statements) *dqlstmt.Statement {
	for _, stmt := range statements {
		if stmt == nil {
			continue
		}
		if stmt.Kind == dqlstmt.KindRead {
			return stmt
		}
	}
	return nil
}

func hasExecStatement(statements dqlstmt.Statements) bool {
	for _, stmt := range statements {
		if stmt != nil && stmt.IsExec {
			return true
		}
	}
	return false
}

func parseQueryWithDiagnosticAt(sqlText, original string, baseOffset int) (*query.Select, *shape.Diagnostic, error) {
	cursor := parsly.NewCursor("", []byte(sqlText), 0)
	var diagnostic *shape.Diagnostic
	cursor.OnError = func(err error, cur *parsly.Cursor, _ interface{}) error {
		offset := 0
		if cur != nil {
			offset = cur.Pos
		}
		if offset < 0 {
			offset = 0
		}
		offset += baseOffset
		diagnostic = &shape.Diagnostic{
			Code:     dqldiag.CodeParseSyntax,
			Severity: shape.SeverityError,
			Message:  strings.TrimSpace(err.Error()),
			Hint:     "check SQL syntax near the reported location",
			Span:     dqlpre.PointSpan(original, offset),
		}
		return err
	}
	result := &query.Select{}
	err := sqlparser.Parse(cursor, result)
	if err != nil {
		if diagnostic == nil {
			diagnostic = &shape.Diagnostic{
				Code:     dqldiag.CodeParseSyntax,
				Severity: shape.SeverityError,
				Message:  strings.TrimSpace(err.Error()),
				Hint:     "check SQL syntax near the reported location",
				Span:     dqlpre.PointSpan(original, baseOffset),
			}
		}
		return nil, diagnostic, errors.New(diagnostic.Error())
	}
	return result, nil, nil
}
