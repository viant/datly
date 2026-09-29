package shared

func EnsureArgs(query string, args *[]interface{}) {
	parameterCount := countPlaceholders(query)
	for i := len(*args); i < parameterCount; i++ { //ensure parameters
		*args = append(*args, "")
	}
}

func countPlaceholders(query string) int {
	count := 0
	inSingle := false
	inDouble := false
	inBacktick := false
	inLineComment := false
	inBlockComment := false

	for i := 0; i < len(query); i++ {
		ch := query[i]

		if inLineComment {
			if ch == '\n' || ch == '\r' {
				inLineComment = false
			}
			continue
		}
		if inBlockComment {
			if ch == '*' && i+1 < len(query) && query[i+1] == '/' {
				inBlockComment = false
				i++
			}
			continue
		}
		if inSingle {
			if ch == '\\' {
				if i+1 < len(query) {
					i++
				}
				continue
			}
			if ch == '\'' {
				inSingle = false
			}
			continue
		}
		if inDouble {
			if ch == '\\' {
				if i+1 < len(query) {
					i++
				}
				continue
			}
			if ch == '"' {
				inDouble = false
			}
			continue
		}
		if inBacktick {
			if ch == '`' {
				inBacktick = false
			}
			continue
		}

		if ch == '-' && i+1 < len(query) && query[i+1] == '-' {
			inLineComment = true
			i++
			continue
		}
		if ch == '/' && i+1 < len(query) && query[i+1] == '*' {
			inBlockComment = true
			i++
			continue
		}

		switch ch {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '`':
			inBacktick = true
		case '?':
			count++
		}
	}
	return count
}
