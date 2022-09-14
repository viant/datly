package router

import (
	"fmt"
	"net/http"
	"strings"
)

type (
	Matchable interface {
		HttpURI() string
		HttpMethod() string
		CorsEnabled() bool
	}

	Matcher struct {
		Nodes       []*Node
		Matchables  []Matchable
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

func (n *Node) Match(method, route string, exact bool, dest *[]*Node) {
	if route == "" {
		*dest = append(*dest, n)
		return
	}

	segment, path := extractSegment(route)

	node, ok := n.nextMatcher(segment)
	if ok {
		node.Match(method, path, exact, dest)
		return
	}

	if n.WildcardMatcher == nil && len(n.ExactChildren) == 0 && !exact {
		*dest = append(*dest, n)
	}
}

func (n *Node) nextMatcher(segment string) (*Node, bool) {
	index, ok := n.index[segment]
	if !ok && n.WildcardMatcher != nil {
		return n.WildcardMatcher, true
	}

	if ok {
		return n.ExactChildren[index], true
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

func (m *Matcher) MatchAllRoutes(method, route string) ([]*Route, error) {
	matched, err := m.match(method, route, true)
	if err != nil {
		return nil, err
	}

	routesSize := 0

	for _, node := range matched {
		routesSize += len(node.Matched)
	}

	routes := make([]*Route, 0, routesSize)

	for _, matchedNode := range matched {
		for _, matchedIndex := range matchedNode.Matched {
			aRoute, ok := m.Matchables[matchedIndex].(*Route)
			if !ok {
				continue
			}

			routes = append(routes, aRoute)

		}
	}

	return routes, nil
}

func (m *Matcher) MatchOneRoute(method, route string) (*Route, error) {
	matched, err := m.match(method, route, true)
	if err != nil {
		return nil, err
	}

	if len(matched) == 0 || (len(matched) == 1 && len(matched[0].Matched) == 0) {
		return nil, m.unmatchedRouteErr(route)
	}

	if len(matched) > 1 || len(matched[0].Matched) > 1 {
		return nil, fmt.Errorf("matched more than one route for %v", route)
	}

	return asRoute(m.firstMatched(matched[0]))
}

func asRoute(matchable Matchable) (*Route, error) {
	aRoute, ok := matchable.(*Route)
	if !ok {
		return nil, fmt.Errorf("unexpected Matcher type, wanted: %T, got %T", aRoute, matchable)
	}

	return aRoute, nil
}

func (m *Matcher) match(method string, route string, exact bool) ([]*Node, error) {
	relative := AsRelative(route)

	methodMatcher, ok := m.getMethodMatcher(method)
	if !ok {
		return nil, m.unmatchedRouteErr(route)
	}

	var matched []*Node
	methodMatcher.Match(method, relative, exact, &matched)
	if len(matched) == 0 {
		return nil, fmt.Errorf("couldn't match URI %v", route)
	}

	return matched, nil
}

func (m *Matcher) unmatchedRouteErr(route string) error {
	return fmt.Errorf("couldn't match URI %v", route)
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
	for i, route := range m.Matchables {
		uri := AsRelative(route.HttpURI())

		node := m.getOrCreateMatcher(route.HttpMethod())
		node.Add(i, uri)

		allUriNodes := m.getOrCreateMatcher("")
		allUriNodes.Add(i, uri)

		if route.CorsEnabled() {
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

func (m *Matcher) MatchPrefix(method string, uriPath string) ([]Matchable, error) {
	allMatch, err := m.match(method, uriPath, false)
	if err != nil {
		return nil, err
	}

	return m.flatten(allMatch), nil
}

func (m *Matcher) firstMatched(match *Node) Matchable {
	return m.Matchables[match.Matched[0]]
}

func (m *Matcher) flatten(match []*Node) []Matchable {
	totalMatched := 0
	for _, node := range match {
		totalMatched += len(node.Matched)
	}

	matchables := make([]Matchable, 0, totalMatched)
	for _, node := range match {
		for _, i := range node.Matched {
			matchables = append(matchables, m.Matchables[i])
		}
	}

	return matchables
}

func AsRelative(route string) string {
	if len(route) == 0 {
		return route
	}

	if route[0] == '/' {
		route = route[1:]
	}
	if paramsStartIndex := strings.IndexByte(route, '?'); paramsStartIndex != -1 {
		route = route[:paramsStartIndex]
	}

	return route
}

func NewRouteMatcher(routes []*Route) *Matcher {
	m := &Matcher{
		Matchables: asMatchables(routes),
	}

	m.init()
	return m
}

func NewMatcher(matchables []Matchable) *Matcher {
	m := &Matcher{
		Matchables: matchables,
	}

	m.init()
	return m
}

func asMatchables(routes []*Route) []Matchable {
	matchables := make([]Matchable, len(routes))
	for i := range routes {
		matchables[i] = routes[i]
	}

	return matchables
}
