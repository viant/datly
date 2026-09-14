package main

import (
	"bytes"
	"fmt"
	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/text"
	"net/url"
	"sort"
	"strings"
)

type markdownLink struct {
	target      string
	start, stop int
}

// Link edits are restricted to parser-confirmed inline destination spans.
// Unsupported source forms fail rather than rewriting code examples or prose.
func markdownLinks(data []byte) ([]markdownLink, map[string]bool, error) {
	document := goldmark.New(goldmark.WithParserOptions(parser.WithAutoHeadingID())).Parser().Parse(text.NewReader(data))
	var result []markdownLink
	anchors := map[string]bool{}
	err := ast.Walk(document, func(node ast.Node, entering bool) (ast.WalkStatus, error) {
		if !entering {
			return ast.WalkContinue, nil
		}
		if heading, ok := node.(*ast.Heading); ok {
			if id, ok := heading.AttributeString("id"); ok {
				anchors[string(id.([]byte))] = true
			}
		}
		var destination []byte
		switch link := node.(type) {
		case *ast.Link:
			destination = link.Destination
		case *ast.Image:
			destination = link.Destination
		default:
			return ast.WalkContinue, nil
		}
		end := 0
		_ = ast.Walk(node, func(child ast.Node, enter bool) (ast.WalkStatus, error) {
			if enter {
				if span, ok := child.(*ast.Text); ok && span.Segment.Stop > end {
					end = span.Segment.Stop
				}
			}
			return ast.WalkContinue, nil
		})
		if end == 0 {
			return ast.WalkStop, fmt.Errorf("link has no source label: %s", destination)
		}
		close := end
		for close < len(data) && strings.ContainsRune("*_~`", rune(data[close])) {
			close++
		}
		if close+1 >= len(data) || data[close] != ']' || data[close+1] != '(' {
			return ast.WalkStop, fmt.Errorf("unsupported link form: %s", destination)
		}
		start := close + 2
		for start < len(data) && (data[start] == ' ' || data[start] == '\n' || data[start] == '\t') {
			start++
		}
		if start < len(data) && data[start] == '<' {
			start++
		}
		stop := start + len(destination)
		if stop > len(data) || !bytes.Equal(data[start:stop], destination) {
			return ast.WalkStop, fmt.Errorf("unsupported link destination encoding: %s", destination)
		}
		result = append(result, markdownLink{target: string(destination), start: start, stop: stop})
		return ast.WalkContinue, nil
	})
	return result, anchors, err
}

func rewriteLinks(data []byte, resolve func(string) (string, error)) ([]byte, error) {
	links, _, err := markdownLinks(data)
	if err != nil {
		return nil, err
	}
	sort.Slice(links, func(i, j int) bool { return links[i].start > links[j].start })
	result := append([]byte(nil), data...)
	for _, link := range links {
		target, err := resolve(link.target)
		if err != nil {
			return nil, err
		}
		result = append(append(append([]byte(nil), result[:link.start]...), []byte(target)...), result[link.stop:]...)
	}
	return result, nil
}

func localReference(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if u.IsAbs() {
		if u.Scheme != "https" && u.Scheme != "http" && u.Scheme != "mailto" {
			return nil, fmt.Errorf("unsupported external reference %s", raw)
		}
		return nil, nil
	}
	if u.Host != "" || u.RawQuery != "" || strings.HasPrefix(u.Path, "/") || strings.ContainsAny(u.Path, "\\\x00") {
		return nil, fmt.Errorf("invalid local reference %s", raw)
	}
	return u, nil
}
