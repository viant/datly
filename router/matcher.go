package router

import (
	"fmt"
	"net/http"
	"strings"
)

type (
	Matcher struct {
		Nodes       []*Node
		Routes      []*Route
		methodIndex map[string]int
	}

	Node struct {
		Matched         []int
		ExactChildren   []*Node
		WildcardMatcher *Node
		index           map[string]int
	}

	NodeMatch struct {
		URL  string
		Node *Node
	}
)

func (n *Node) Add(routeIndex int, uri string) {
	if uri == "" {
		n.Matched = append(n.Matched, routeIndex)
		return
	}

	segment, remaining := extractSegment(uri)
	var child *Node
	if segment[0] == '{' {
		child = n.getWildcardMatcher()
	} else {
		child = n.getChildOrCreate(segment)
	}

	child.Add(routeIndex, remaining)
}

func (n *Node) getChildOrCreate(segment string) *Node {
	if childIndex, ok := n.index[segment]; ok {
		return n.ExactChildren[childIndex]
	}

	n.index[segment] = len(n.ExactChildren)
	child := NewNode()
	n.ExactChildren = append(n.ExactChildren, child)
	return child
}

func NewNode() *Node {
	return &Node{index: map[string]int{}}
}

func (n *Node) Match(method, route string) (*Node, bool) {
	if route == "" {
		return n, true
	}

	segment, path := extractSegment(route)

	index, ok := n.index[segment]
	if !ok && n.WildcardMatcher != nil {
		return n.WildcardMatcher.Match(method, path)
	}

	if ok {
		return n.ExactChildren[index].Match(method, path)
	}

	return nil, false
}

func (n *Node) getWildcardMatcher() *Node {
	if n.WildcardMatcher != nil {
		return n.WildcardMatcher
	}

	n.WildcardMatcher = &Node{
		index: map[string]int{},
	}

	return n.WildcardMatcher
}

func extractSegment(uri string) (string, string) {
	if segIndex := strings.IndexByte(uri, '/'); segIndex != -1 {
		return uri[:segIndex], uri[segIndex+1:]
	}

	return uri, ""
}

func (m *Matcher) Match(method, route string) (*Route, error) {
	relative := asRelative(route)

	methodMatcher, ok := m.getMethodMatcher(method)
	if !ok {
		return nil, fmt.Errorf("couldn't match URI %v", route)
	}

	matched, ok := methodMatcher.Match(method, relative)
	if !ok || len(matched.Matched) == 0 {
		return nil, fmt.Errorf("couldn't match URI %v", route)
	}

	if len(matched.Matched) > 1 {
		return nil, fmt.Errorf("matched more than one route for %v", route)
	}

	return m.Routes[matched.Matched[0]], nil
}

func (m *Matcher) getMethodMatcher(method string) (*Node, bool) {
	index, ok := m.methodIndex[method]
	if !ok {
		return nil, false
	}

	return m.Nodes[index], true
}

func (m *Matcher) init() {
	m.methodIndex = map[string]int{}
	for i, route := range m.Routes {
		uri := asRelative(route.URI)

		node := m.getOrCreateMatcher(route.Method)
		node.Add(i, uri)

		if route.Cors != nil {
			corsMatcher := m.getOrCreateMatcher(http.MethodOptions)
			corsMatcher.Add(i, uri)
		}
	}
}

func (m *Matcher) getOrCreateMatcher(method string) *Node {
	matcher, ok := m.getMethodMatcher(method)
	if ok {
		return matcher
	}

	node := NewNode()
	m.methodIndex[method] = len(m.Nodes)
	m.Nodes = append(m.Nodes, node)
	return node
}

func asRelative(route string) string {
	if route[0] == '/' {
		route = route[1:]
	}

	if paramsStartIndex := strings.IndexByte(route, '?'); paramsStartIndex != -1 {
		route = route[:paramsStartIndex]
	}

	return route
}

func NewMatcher(routes []*Route) *Matcher {
	m := &Matcher{
		Routes: routes,
	}

	m.init()
	return m
}
