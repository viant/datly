package debuglog

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

const initEnv = "DATLY_DEBUG_INIT_JSON"

func Enabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv(initEnv)), "1") ||
		strings.EqualFold(strings.TrimSpace(os.Getenv(initEnv)), "true")
}

func JSON(event string, fields map[string]any) {
	if !Enabled() {
		return
	}
	payload := map[string]any{
		"event": event,
	}
	for k, v := range fields {
		payload[k] = v
	}
	data, err := json.Marshal(payload)
	if err != nil {
		fmt.Printf("{\"event\":\"debuglog.marshal_error\",\"error\":%q}\n", err.Error())
		return
	}
	fmt.Println(string(data))
}

func YAMLFailure(location, sourceURL string, data []byte, err error) {
	if !Enabled() {
		return
	}
	preview := strings.TrimSpace(string(data))
	if len(preview) > 512 {
		preview = preview[:512]
	}
	JSON("yaml.unmarshal.failure", map[string]any{
		"location": location,
		"source":   sourceURL,
		"error":    err.Error(),
		"preview":  preview,
	})
}
