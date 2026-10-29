package cubecompose

import (
	"fmt"
	"strconv"
)

// Page is an explicit transport window over a compiled composition. Omit the
// optional input Page to retain the authored SQL window. An explicit zero or
// empty Page is invalid; Limit must be positive and Offset must be nonnegative.
type Page struct {
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
}

// WithPage returns a detached plan with a transport window. It preserves the
// compiled selection and semantic result limit. The native response budget
// includes skipped rows; bounded lookahead is the only extra physical row.
func (p *Plan) WithPage(page Page) (*Plan, error) {
	if p == nil || p.selectQuery == nil || p.maxLimit <= 0 {
		return nil, fmt.Errorf("compiled cube compose plan required")
	}
	if page.Limit <= 0 || page.Offset < 0 || page.Offset > p.maxLimit || page.Limit > p.maxLimit-page.Offset || page.Limit == int(^uint(0)>>1) {
		return nil, fmt.Errorf("cube compose page requires positive LIMIT and nonnegative OFFSET with LIMIT plus OFFSET at most %d and room for lookahead", p.maxLimit)
	}
	order := p.selectQuery.OrderBy
	if len(p.unionBranches) > 0 {
		order = p.unionOrder
	}
	if page.Offset > 0 && len(order) == 0 {
		return nil, fmt.Errorf("cube compose page OFFSET requires explicit ordering")
	}
	result := *p
	result.Limit, result.offset = page.Limit, page.Offset
	result.Columns = append([]Column(nil), p.Columns...)
	result.Fields = make([][]string, len(p.Fields))
	for i, fields := range p.Fields {
		result.Fields[i] = append([]string(nil), fields...)
	}
	return &result, nil
}

// Offset is the offset in the combined result.
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
