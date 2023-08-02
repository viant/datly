package shared

import "strings"

func TrimPair(text string, begin, end byte) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return text
	}
	if text[0] == begin {
		text = text[1:]
		if text[len(text)-1] == end {
			text = text[:len(text)-1]
		}
	}
	return text
}
