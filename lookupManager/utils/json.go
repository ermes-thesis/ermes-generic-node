package utils

import (
	"bytes"
	"encoding/json"
	"strings"
)

// Marshal https://stackoverflow.com/a/69502657
func Marshal(i any) ([]byte, error) {
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(i)
	return bytes.TrimRight(buffer.Bytes(), "\n"), err
}

func escapeString(s string) string {
	return strings.ReplaceAll(s, `"`, `\"`)
}
