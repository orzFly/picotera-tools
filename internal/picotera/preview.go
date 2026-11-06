package picotera

import (
	"bytes"
	"encoding/json"
	"io"
)

// Format names the client-facing wire format, matching llmbridge's
// Format.String() values in picotera.
type Format string

const (
	FormatAnthropicMessages           Format = "anthropicMessages"
	FormatOpenAIChatCompletions       Format = "openaiChatCompletions"
	FormatOpenAIResponses             Format = "openaiResponses"
	FormatGeminiStreamGenerateContent Format = "geminiStreamGenerateContent"
	FormatUnknown                     Format = ""
)

// UserMessagePreview mirrors picotera's extractUserMessagePreview, keyed by
// format instead of endpoint type.
func UserMessagePreview(body []byte, format Format) (string, bool) {
	text, ok := extractUserMessage(body, format)
	if !ok {
		return "", false
	}
	return shortenUserMessagePreview(text), true
}

func extractUserMessage(body []byte, format Format) (string, bool) {
	switch format {
	case FormatOpenAIChatCompletions:
		return extractOpenAIChatUserMessage(body)
	case FormatAnthropicMessages:
		return extractAnthropicUserMessage(body)
	case FormatOpenAIResponses:
		return extractOpenAIResponsesUserMessage(body)
	case FormatGeminiStreamGenerateContent:
		return extractGeminiUserMessage(body)
	}
	for _, fn := range []func([]byte) (string, bool){
		extractOpenAIChatUserMessage,
		extractAnthropicUserMessage,
		extractOpenAIResponsesUserMessage,
		extractGeminiUserMessage,
		extractQueryUserMessage,
	} {
		if text, ok := fn(body); ok {
			return text, true
		}
	}
	return "", false
}

func extractQueryUserMessage(body []byte) (string, bool) {
	root, ok := decodeJSONObject(body)
	if !ok {
		return "", false
	}
	q, ok := root["query"].(string)
	if !ok || q == "" {
		return "", false
	}
	return q, true
}

func shortenUserMessagePreview(text string) string {
	runes := []rune(text)
	if len(runes) <= 30 {
		return text
	}
	return string(runes[:15]) + "..." + string(runes[len(runes)-15:])
}

func startsWithMarkup(text string) bool {
	return len(text) > 0 && text[0] == '<'
}

func decodeJSONObject(body []byte) (map[string]any, bool) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var root map[string]any
	if err := dec.Decode(&root); err != nil {
		return nil, false
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, false
	}
	return root, true
}

func extractOpenAIChatUserMessage(body []byte) (string, bool) {
	root, ok := decodeJSONObject(body)
	if !ok {
		return "", false
	}
	return extractRoleMessage(root["messages"], "user", extractTextContent)
}

func extractAnthropicUserMessage(body []byte) (string, bool) {
	root, ok := decodeJSONObject(body)
	if !ok {
		return "", false
	}
	return extractRoleMessage(root["messages"], "user", extractTextContent)
}

func extractOpenAIResponsesUserMessage(body []byte) (string, bool) {
	root, ok := decodeJSONObject(body)
	if !ok {
		return "", false
	}
	switch input := root["input"].(type) {
	case string:
		return input, true
	case []any:
		for i := len(input) - 1; i >= 0; i-- {
			item, ok := input[i].(map[string]any)
			if !ok || item["role"] != "user" {
				continue
			}
			if text, ok := extractInputTextContent(item["content"]); ok {
				return text, true
			}
		}
	}
	return "", false
}

func extractGeminiUserMessage(body []byte) (string, bool) {
	root, ok := decodeJSONObject(body)
	if !ok {
		return "", false
	}
	contents, ok := root["contents"].([]any)
	if !ok {
		return "", false
	}
	for i := len(contents) - 1; i >= 0; i-- {
		item, ok := contents[i].(map[string]any)
		if !ok || item["role"] != "user" {
			continue
		}
		if text, ok := extractGeminiParts(item["parts"]); ok {
			return text, true
		}
	}
	return "", false
}

func extractRoleMessage(messagesValue any, role string, contentExtractor func(any) (string, bool)) (string, bool) {
	messages, ok := messagesValue.([]any)
	if !ok {
		return "", false
	}
	for i := len(messages) - 1; i >= 0; i-- {
		msg, ok := messages[i].(map[string]any)
		if !ok || msg["role"] != role {
			continue
		}
		if text, ok := contentExtractor(msg["content"]); ok {
			return text, true
		}
	}
	return "", false
}

func extractTextContent(content any) (string, bool) {
	switch c := content.(type) {
	case string:
		return c, true
	case []any:
		for i := len(c) - 1; i >= 0; i-- {
			part, ok := c[i].(map[string]any)
			if !ok || part["type"] != "text" {
				continue
			}
			if text, ok := part["text"].(string); ok {
				if startsWithMarkup(text) {
					continue
				}
				return text, true
			}
		}
	}
	return "", false
}

func extractInputTextContent(content any) (string, bool) {
	switch c := content.(type) {
	case string:
		return c, true
	case []any:
		for i := len(c) - 1; i >= 0; i-- {
			part, ok := c[i].(map[string]any)
			if !ok || part["type"] != "input_text" {
				continue
			}
			if text, ok := part["text"].(string); ok {
				if startsWithMarkup(text) {
					continue
				}
				return text, true
			}
		}
	}
	return "", false
}

func extractGeminiParts(parts any) (string, bool) {
	arr, ok := parts.([]any)
	if !ok {
		return "", false
	}
	for i := len(arr) - 1; i >= 0; i-- {
		part, ok := arr[i].(map[string]any)
		if !ok {
			continue
		}
		if text, ok := part["text"].(string); ok {
			if startsWithMarkup(text) {
				continue
			}
			return text, true
		}
	}
	return "", false
}
