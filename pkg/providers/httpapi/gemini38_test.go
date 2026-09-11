package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGemini38MediumPDFRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		gen := body["generationConfig"].(map[string]any)
		if _, exists := gen["temperature"]; exists {
			t.Error("unsupported temperature sent to Gemini 3.8")
		}
		thinking := gen["thinkingConfig"].(map[string]any)
		if thinking["thinkingLevel"] != "medium" {
			t.Errorf("thinking config: %v", thinking)
		}
		contents := body["contents"].([]any)
		parts := contents[0].(map[string]any)["parts"].([]any)
		found := false
		for _, p := range parts {
			if inline, ok := p.(map[string]any)["inlineData"].(map[string]any); ok {
				found = inline["mimeType"] == "application/pdf"
			}
		}
		if !found {
			t.Error("PDF missing from native inlineData")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"ok"}]},"finishReason":"STOP"}]}`))
	}))
	defer server.Close()
	p := NewGeminiProvider("test", server.URL, "", "test", 0, nil, nil)
	_, err := p.Chat(t.Context(), []Message{{Role: "user", Content: "Read PDF", Media: []string{"data:application/pdf;base64,JVBERi0xLjc="}}}, nil, "gemini-3.8-flash", map[string]any{"thinking_level": "medium", "temperature": 0.7})
	if err != nil {
		t.Fatal(err)
	}
}

func TestGemini38DoesNotSendMinimal(t *testing.T) {
	for _, level := range []string{"", "off", "minimal"} {
		got := buildGeminiThinkingConfig("gemini-3.8-flash", map[string]any{"thinking_level": level})
		if _, exists := got["thinkingLevel"]; exists {
			t.Fatalf("unsupported level for %q: %v", level, got)
		}
	}
	got := buildGeminiThinkingConfig("gemini-3.5-flash", map[string]any{"thinking_level": "minimal"})
	if got["thinkingLevel"] != "minimal" {
		t.Fatal("changed Gemini 3.5 behavior")
	}
}
