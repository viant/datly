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
	authSchemePattern         = regexp.MustCompile(`(?i)\b(bearer|basic)([ \t]+)[A-Za-z0-9._~+/=-]+`)
	credentialPattern         = regexp.MustCompile(`(?i)\b(authorization|proxy[-_ ]?authorization|refresh[-_ ]?token|access[-_ ]?token|id[-_ ]?token|api[-_ ]?key|client[-_ ]?secret|password|passwd|cookie|set-cookie|token)\b(\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^\s,;}\]&]+)`)
	jwtPattern                = regexp.MustCompile(`\b[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b`)
)

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
	case "authorization", "proxyauthorization", "refreshtoken", "accesstoken", "idtoken", "authtoken", "token", "apikey", "clientsecret", "password", "passwd", "cookie", "setcookie":
		return true
	}
	return strings.Contains(value, "authorization") || strings.HasSuffix(value, "apikey") || strings.HasSuffix(value, "secret") || strings.HasSuffix(value, "password")
}
