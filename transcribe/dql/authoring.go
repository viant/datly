package dql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
)

// DeclarationOccurrence retains one authored declaration and the source ranges
// needed by source-preserving editor clients. Parameter is detached metadata;
// Span and predicate spans address the complete authored DQL.
type DeclarationOccurrence struct {
	Parameter    *spec.Parameter               `json:"parameter"`
	Span         SourceSpan                    `json:"span"`
	HeadSpan     SourceSpan                    `json:"headSpan"`
	OptionInsert int                           `json:"optionInsert"`
	Options      []DeclarationOptionOccurrence `json:"options,omitempty"`
	Predicates   []PredicateOptionOccurrence   `json:"predicates,omitempty"`
}

type DeclarationOptionOccurrence struct {
	Name string     `json:"name"`
	Args []string   `json:"args,omitempty"`
	Span SourceSpan `json:"span"`
}

// PredicateOptionOccurrence identifies one repeatable predicate option in its
// declaration. Ordinal is scoped to the declaration and follows source order.
type PredicateOptionOccurrence struct {
	Ordinal   int             `json:"ordinal"`
	Predicate *spec.Predicate `json:"predicate"`
	Span      SourceSpan      `json:"span"`
}

// DeclarationOccurrences parses every authored parameter declaration without
// applying #define-over-#set precedence. This is an authoring projection; the
// canonical component continues to use the normalized declaration plan.
func DeclarationOccurrences(source string) ([]DeclarationOccurrence, error) {
	blocks := extractDirectiveBlocks(source)
	result := make([]DeclarationOccurrence, 0)
	for _, block := range blocks {
		declaration, ok := toDeclarationKind(block.kind)
		if !ok || isCubeSetting(block) {
			continue
		}
		holder, _, location, tail, tailOffset, ok := parseDeclarationHead(block.body)
		implicit := false
		if !ok {
			holder, tail, tailOffset, ok = parseImplicitDeclarationHead(block.body)
			implicit = ok
			location = holder
		}
		if !ok {
			if declaration == spec.DeclarationKindDefine {
				return nil, fmt.Errorf("invalid #define declaration at offset %d", block.start)
			}
			continue
		}
		parsed, _, _, err := parseDeclarations([]directiveBlock{block})
		if err != nil {
			return nil, err
		}
		if len(parsed) != 1 {
			continue
		}
		parameter := parsed[0]
		parameter.Declaration = declaration
		if implicit && parameter.Source.Name == "" {
			parameter.Source.Name = location
		}
		occurrence := DeclarationOccurrence{
			Parameter: parameter.Clone(),
			Span:      SourceSpan{Start: block.start, End: block.end},
		}
		if !implicit {
			if head, headOK := declarationHeadSpan(block.body); headOK {
				occurrence.HeadSpan = SourceSpan{Start: block.bodyStart + head.Start, End: block.bodyStart + head.End}
			}
		}
		cursor := newOptionCursor(tail)
		ordinal := 0
		for cursor.next() {
			name, args := cursor.option()
			key := strings.ToLower(strings.TrimSpace(name))
			optionSpan := SourceSpan{Start: block.bodyStart + tailOffset + cursor.start, End: block.bodyStart + tailOffset + cursor.cursor}
			occurrence.Options = append(occurrence.Options, DeclarationOptionOccurrence{Name: name, Args: append([]string(nil), args...), Span: optionSpan})
			if key != "withpredicate" && key != "predicate" && key != "applywhenabsentpredicate" {
				continue
			}
			predicate := (&declarationOptionParser{}).predicate(args, key == "applywhenabsentpredicate")
			if predicate == nil || strings.TrimSpace(predicate.Name) == "" {
				return nil, fmt.Errorf("invalid predicate option for %s", holder)
			}
			occurrence.Predicates = append(occurrence.Predicates, PredicateOptionOccurrence{
				Ordinal:   ordinal,
				Predicate: predicate,
				Span:      optionSpan,
			})
			ordinal++
		}
		if err := cursor.Err(); err != nil {
			return nil, err
		}
		occurrence.OptionInsert = block.bodyStart + tailOffset + cursor.cursor
		result = append(result, occurrence)
	}
	return result, nil
}

func declarationHeadSpan(body string) (SourceSpan, bool) {
	cursor := newByteCursor(body)
	skipWhitespace(cursor)
	if !consumeAssignmentPrefix(cursor) {
		return SourceSpan{}, false
	}
	skipWhitespace(cursor)
	start := cursor.pos
	if cursor.pos >= len(cursor.input) || cursor.input[cursor.pos] != '$' {
		return SourceSpan{}, false
	}
	cursor.pos++
	if _, ok := readIdentifier(cursor); !ok {
		return SourceSpan{}, false
	}
	skipWhitespace(cursor)
	if cursor.pos < len(cursor.input) && cursor.input[cursor.pos] == '<' {
		if _, ok := readBracketGroup(cursor, '<', '>'); !ok {
			return SourceSpan{}, false
		}
	}
	skipWhitespace(cursor)
	if _, ok := readBracketGroup(cursor, '(', ')'); !ok {
		return SourceSpan{}, false
	}
	return SourceSpan{Start: start, End: cursor.pos}, true
}

// RenderPredicateOption renders the canonical declaration option spelling.
func RenderPredicateOption(predicate *spec.Predicate) (string, error) {
	if predicate == nil || strings.TrimSpace(predicate.Name) == "" {
		return "", fmt.Errorf("predicate name is required")
	}
	name := "WithPredicate"
	if predicate.ApplyWhenAbsent {
		name = "ApplyWhenAbsentPredicate"
	}
	args := []string{strconv.Itoa(predicate.Group), strconv.Quote(strings.TrimSpace(predicate.Name))}
	for _, arg := range predicate.Args {
		args = append(args, strconv.Quote(arg))
	}
	return "." + name + "(" + strings.Join(args, ", ") + ")", nil
}

// ApplyPatch applies one half-open byte patch to authored DQL.
func ApplyPatch(source string, span SourceSpan, replacement string) (string, error) {
	if span.Start < 0 || span.End < span.Start || span.End > len(source) {
		return "", fmt.Errorf("invalid source span [%d:%d]", span.Start, span.End)
	}
	return source[:span.Start] + replacement + source[span.End:], nil
}

// UpsertSetting replaces every authored setting call with the same canonical
// name by one call at the first occurrence, or inserts a new call before SQL.
// A nil args slice removes the setting. Arguments are already-authored DQL
// expressions and are retained verbatim after trimming.
func UpsertSetting(source, name string, args []string) (string, error) {
	return UpsertSettingTail(source, name, args, "")
}

func UpsertSettingTail(source, name string, args []string, tail string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("setting name is required")
	}
	var matches []SourceSpan
	for _, block := range extractDirectiveBlocks(source) {
		if block.kind != directiveKindSetting {
			continue
		}
		actual, _, _, ok := parseDirectiveCall(block.body)
		if ok && strings.EqualFold(strings.TrimSpace(actual), name) {
			matches = append(matches, SourceSpan{Start: block.start, End: block.end})
		}
	}
	replacement := ""
	if args != nil {
		for _, arg := range args {
			if strings.TrimSpace(arg) == "" {
				return "", fmt.Errorf("setting %s has an empty argument", name)
			}
		}
		replacement = "#setting($_ = $" + name + "(" + strings.Join(args, ", ") + ")" + strings.TrimSpace(tail) + ")"
	}
	if len(matches) == 0 {
		if args == nil {
			return source, nil
		}
		prepared := PrepareSource(source)
		if err := prepared.Err(); err != nil {
			return "", err
		}
		return ApplyPatch(source, SourceSpan{Start: prepared.TrimPrefix, End: prepared.TrimPrefix}, replacement+"\n")
	}
	result := source
	for index := len(matches) - 1; index >= 0; index-- {
		text := ""
		if index == 0 {
			text = replacement
		}
		var err error
		result, err = ApplyPatch(result, matches[index], text)
		if err != nil {
			return "", err
		}
	}
	return result, nil
}
