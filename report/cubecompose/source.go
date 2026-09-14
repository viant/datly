package cubecompose

import (
	"fmt"
	"strconv"
	"strings"
)

func cubeMacro(frame int) string {
	return "$CubeSQL" + strconv.Itoa(frame)
}

func cubeSource(frame int) string {
	return sourcePrefix + strconv.Itoa(frame)
}

func cubeAlias(frame int) string {
	return "t" + strconv.Itoa(frame)
}

func replaceCubeMacros(SQL string, frameCount int) (string, error) {
	var result strings.Builder
	result.Grow(len(SQL))
	for i := 0; i < len(SQL); {
		switch {
		case SQL[i] == '\'' || SQL[i] == '"' || SQL[i] == '`':
			next, err := copyQuotedSQL(&result, SQL, i, SQL[i])
			if err != nil {
				return "", err
			}
			i = next
		case i+1 < len(SQL) && SQL[i:i+2] == "--":
			next := strings.IndexByte(SQL[i+2:], '\n')
			if next == -1 {
				result.WriteString(SQL[i:])
				return result.String(), nil
			}
			next += i + 3
			result.WriteString(SQL[i:next])
			i = next
		case i+1 < len(SQL) && SQL[i:i+2] == "/*":
			next := strings.Index(SQL[i+2:], "*/")
			if next == -1 {
				return "", fmt.Errorf("invalid cube compose SQL: unterminated block comment")
			}
			next += i + 4
			result.WriteString(SQL[i:next])
			i = next
		case strings.HasPrefix(SQL[i:], "$CubeSQL"):
			end := i + len("$CubeSQL")
			for end < len(SQL) && SQL[end] >= '0' && SQL[end] <= '9' {
				end++
			}
			token := SQL[i:end]
			if end == i+len("$CubeSQL") || end < len(SQL) && isIdentifierByte(SQL[end]) {
				return "", fmt.Errorf("invalid cube compose macro %q", token)
			}
			frame, err := strconv.Atoi(SQL[i+len("$CubeSQL") : end])
			if err != nil || frame < 1 || frame > frameCount || token != cubeMacro(frame) {
				return "", fmt.Errorf("cube compose macro %q does not identify one of the %d submitted cubes", token, frameCount)
			}
			result.WriteString(cubeSource(frame))
			i = end
		default:
			result.WriteByte(SQL[i])
			i++
		}
	}
	return result.String(), nil
}

func copyQuotedSQL(result *strings.Builder, SQL string, start int, quote byte) (int, error) {
	result.WriteByte(quote)
	for i := start + 1; i < len(SQL); i++ {
		result.WriteByte(SQL[i])
		if SQL[i] == '\\' && i+1 < len(SQL) {
			i++
			result.WriteByte(SQL[i])
			continue
		}
		if SQL[i] != quote {
			continue
		}
		if i+1 < len(SQL) && SQL[i+1] == quote {
			i++
			result.WriteByte(SQL[i])
			continue
		}
		return i + 1, nil
	}
	return 0, fmt.Errorf("invalid cube compose SQL: unterminated quoted value")
}

func isIdentifierByte(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' || value == '_'
}
