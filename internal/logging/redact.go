package logging

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
	"unicode"
)

const redactedValue = "[REDACTED]"

var (
	sensitiveTransformPattern = regexp.MustCompile(`(?i)(failed\s+to\s+transform\s+(?:authorization|proxy[-_ ]?authorization|refresh[-_ ]?token|access[-_ ]?token|id[-_ ]?token|api[-_ ]?key|client[-_ ]?secret|password|passwd|cookie)\s+with\s+[^:[:space:]]+\s*:).*`)
	authSchemePattern         = regexp.MustCompile(`(?i)\b(bearer|basic)(\s+)[A-Za-z0-9._~+/=-]+`)
	credentialPattern         = regexp.MustCompile(`(?i)\b(authorization|proxy[-_ ]?authorization|refresh[-_ ]?token|access[-_ ]?token|id[-_ ]?token|api[-_ ]?key|client[-_ ]?secret|password|passwd|pwd|dsn|cookie|set-cookie|token)\b(["']?\s*[:=]\s*)(?:\[REDACTED\]|"[^"]*"|'[^']*'|[^\s,;)}\]&]+)`)
	jwtPattern                = regexp.MustCompile(`\b[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b`)
	urlCredentialsPattern     = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://)[^\s/?#]*@`)
	mysqlCredentialsPattern   = regexp.MustCompile(`(?i)\b[^\s:]+:[^\s]*?@(tcp|unix)\(`)
	bareCredentialsPattern    = regexp.MustCompile(`\b[^\s:/"']+:[^\s@"']*@`)
	cookiePattern             = regexp.MustCompile(`(?im)\b(set-cookie|cookie)(["']?\s*[:=]\s*)[^\r\n]*`)
	queryFailurePattern       = regexp.MustCompile(`(?is)(failed\s+to\s+(?:run\s+query|scan)\s*:?\s+).*?(,\s+due\s+to\s+|$)`)
	diagnosticPayloadPattern  = regexp.MustCompile(`(?i)\b(sql|query|args|params|parameters|body|response[-_ ]?body|request[-_ ]?body|headers?|request[-_ ]?headers?|response[-_ ]?headers?)["']?\s*[:=]\s*`)
	inlineSQLPattern          = regexp.MustCompile(`(?is)\bSQL\s+(?:SELECT|INSERT|UPDATE|DELETE|WITH)\b.*`)
	lineBreaks                = strings.NewReplacer("\r", `\r`, "\n", `\n`)
)

// SafeText redacts known credential forms and escapes record delimiters. It is
// shared by native structured and compatibility diagnostics, not public errors.
func SafeText(value string) string {
	return lineBreaks.Replace(redactSensitiveText(value))
}

// FailureReason retains the error chain, excluding query/payload evidence that
// drivers can embed in Error(). It never rewrites the original metric or error.
// SQLX query/scan wrappers are handled even for preparation failures without an
// SQLExecution. Known query text is also removed regardless of IncludeSQL.
func FailureReason(message string, queries ...string) string {
	for _, query := range queries {
		if query != "" {
			message = strings.ReplaceAll(message, query, redactedValue)
		}
	}
	message = queryFailurePattern.ReplaceAllString(message, `${1}`+redactedValue+`${2}`)
	message = inlineSQLPattern.ReplaceAllString(message, "SQL "+redactedValue)
	for _, field := range diagnosticPayloadPattern.FindAllStringIndex(message, -1) {
		// A query wrapper was already stripped; retain its following cause.
		if strings.HasPrefix(message[field[1]:], redactedValue) {
			continue
		}
		message = message[:field[1]] + redactedValue
		break
	}
	return SafeText(message)
}

// redactSerializedJSON sanitizes the final audit/trace payload. Redacting at
// this boundary protects values embedded in wrapped errors as well as future
// fields added to the logging snapshot.
func redactSerializedJSON(data []byte) []byte {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value interface{}
	if err := decoder.Decode(&value); err != nil {
		return []byte(redactSensitiveText(string(data)))
	}
	value = redactJSONValue(value)
	result, err := json.Marshal(value)
	if err != nil {
		return []byte(redactSensitiveText(string(data)))
	}
	return result
}

func redactJSONValue(value interface{}) interface{} {
	switch actual := value.(type) {
	case map[string]interface{}:
		for key, item := range actual {
			if isSensitiveKey(key) {
				actual[key] = redactedValue
				continue
			}
			actual[key] = redactJSONValue(item)
		}
		return actual
	case []interface{}:
		for i, item := range actual {
			actual[i] = redactJSONValue(item)
		}
		return actual
	case string:
		return redactSensitiveText(actual)
	default:
		return value
	}
}

func redactSensitiveText(value string) string {
	if value == "" {
		return value
	}
	value = sensitiveTransformPattern.ReplaceAllString(value, `${1} `+redactedValue)
	value = cookiePattern.ReplaceAllString(value, `${1}${2}`+redactedValue)
	value = urlCredentialsPattern.ReplaceAllString(value, `${1}`+redactedValue+`@`)
	value = mysqlCredentialsPattern.ReplaceAllString(value, redactedValue+`@${1}(`)
	value = bareCredentialsPattern.ReplaceAllString(value, redactedValue+`@`)
	value = authSchemePattern.ReplaceAllString(value, `${1}${2}`+redactedValue)
	value = credentialPattern.ReplaceAllString(value, `${1}${2}`+redactedValue)
	return jwtPattern.ReplaceAllString(value, redactedValue)
}

func isSensitiveKey(key string) bool {
	var normalized strings.Builder
	for _, char := range key {
		if unicode.IsLetter(char) || unicode.IsDigit(char) {
			normalized.WriteRune(unicode.ToLower(char))
		}
	}
	value := normalized.String()
	switch value {
	case "authorization", "proxyauthorization", "refreshtoken", "accesstoken", "idtoken", "authtoken", "token", "apikey", "clientsecret", "password", "passwd", "pwd", "dsn", "cookie", "setcookie":
		return true
	}
	return strings.Contains(value, "authorization") || strings.HasSuffix(value, "apikey") || strings.HasSuffix(value, "secret") || strings.HasSuffix(value, "password")
}
