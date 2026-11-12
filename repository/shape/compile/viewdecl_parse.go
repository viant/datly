package compile

import (
	"strings"
	"unicode"

	"github.com/viant/parsly"
)

type setBlock struct {
	Offset     int
	BodyOffset int
	Body       string
}

func extractSetBlocks(dql string) []setBlock {
	cursor := parsly.NewCursor("", []byte(dql), 0)
	var result []setBlock
	for cursor.Pos < cursor.InputSize {
		matched := cursor.MatchAfterOptional(vdWhitespaceMatcher, vdSetMatcher, vdDefineMatcher)
		if matched.Code != vdSetToken && matched.Code != vdDefineToken {
			cursor.Pos++
			continue
		}
		offset := cursor.Pos - len(matched.Text(cursor))
		group := cursor.MatchAfterOptional(vdWhitespaceMatcher, vdExprGroupMatcher)
		if group.Code != vdExprGroupToken {
			continue
		}
		groupText := group.Text(cursor)
		groupStart := cursor.Pos - len(groupText)
		body := group.Text(cursor)
		if len(body) < 2 {
			continue
		}
		result = append(result, setBlock{
			Offset:     offset,
			BodyOffset: groupStart + 1,
			Body:       body[1 : len(body)-1],
		})
	}
	return result
}

func parseSetDeclarationBody(body string) (holder, kind, location, tail string, tailOffset int, ok bool) {
	cursor := parsly.NewCursor("", []byte(body), 0)
	if cursor.MatchAfterOptional(vdWhitespaceMatcher, vdParamDeclMatcher).Code != vdParamDeclToken {
		return "", "", "", "", 0, false
	}
	id, matched := readIdentifier(cursor)
	if !matched {
		return "", "", "", "", 0, false
	}
	holder = id
	_ = cursor.MatchOne(vdWhitespaceMatcher)
	_ = cursor.MatchOne(vdTypeMatcher)
	_ = cursor.MatchOne(vdWhitespaceMatcher)
	kindLoc := cursor.MatchOne(vdExprGroupMatcher)
	if kindLoc.Code != vdExprGroupToken {
		return "", "", "", "", 0, false
	}
	inGroup := kindLoc.Text(cursor)
	if len(inGroup) < 2 {
		return "", "", "", "", 0, false
	}
	raw := strings.TrimSpace(inGroup[1 : len(inGroup)-1])
	slash := strings.Index(raw, "/")
	if slash == -1 {
		return "", "", "", "", 0, false
	}
	kind = strings.ToLower(strings.TrimSpace(raw[:slash]))
	location = strings.TrimSpace(raw[slash+1:])
	tailOffset = cursor.Pos
	tail = strings.TrimSpace(string(cursor.Input[cursor.Pos:]))
	return holder, kind, location, tail, tailOffset, true
}

func readIdentifier(cursor *parsly.Cursor) (string, bool) {
	if cursor.Pos >= cursor.InputSize {
		return "", false
	}
	start := cursor.Pos
	for cursor.Pos < cursor.InputSize {
		ch := rune(cursor.Input[cursor.Pos])
		if ch == '_' || ch == '$' || unicode.IsLetter(ch) || unicode.IsDigit(ch) {
			cursor.Pos++
			continue
		}
		break
	}
	if cursor.Pos == start {
		return "", false
	}
	return string(cursor.Input[start:cursor.Pos]), true
}
