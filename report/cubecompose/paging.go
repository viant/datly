package cubecompose

import (
	"fmt"
	"strconv"
)

// Offset is the offset in the combined result; ordinary join plans start at zero.
func (p *Plan) Offset() int { return p.offset }

// RenderPage adds one bounded lookahead row to the requested result limit.
// This row is used only to establish hasMore and must not be returned as data.
// The response budget remains Limit; physical reads are bounded by Limit+1.
// Render remains available with its original exact-limit behavior.
func (p *Plan) RenderPage(frames []Frame) (string, []interface{}, error) {
	if p.resultLimit > 0 {
		return p.renderResultPage(frames, true)
	}
	if p.Limit == int(^uint(0)>>1) {
		return "", nil, fmt.Errorf("cube compose limit leaves no room for bounded lookahead")
	}
	page := *p
	page.Limit++
	return page.Render(frames)
}

// Slice an explicitly ordered semantic prefix algebraically: OFFSET plus the
// remaining LIMIT never crosses its end. No extra whole-cohort read or in-memory
// ranking is introduced, and both Render and RenderPage honor the same result.
func (p *Plan) renderResultPage(frames []Frame, lookahead bool) (string, []interface{}, error) {
	remaining := p.resultLimit - p.offset
	if remaining < 0 {
		remaining = 0
	}
	take := p.Limit
	if take >= remaining {
		take = remaining
	} else if lookahead {
		take++
	}
	page := *p
	page.Limit = take
	page.resultLimit = 0
	page.offset = 0
	sql, args, err := page.render(frames)
	if err != nil {
		return "", nil, err
	}
	if p.offset > 0 {
		sql += " OFFSET " + strconv.Itoa(p.offset)
	}
	return sql, args, nil
}
